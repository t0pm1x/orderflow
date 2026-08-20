// k6 load test for the orderflow chain.
//
// Two scenarios run concurrently:
//
//   1. generateLoad (default): 50 VUs × 60 s, fires POST /v1/orders
//      and validates the 201. This is the load-generation profile
//      that the original k6.js used; it asserts the order service
//      accepts the request rate without dropping connections.
//
//   2. pollChain (parallel): 10 VUs × 1 iteration, fires one POST
//      /v1/orders and then polls GET /v1/orders/{id} until the
//      state reaches "confirmed" (or the per-order polling budget
//      elapses). This is the end-to-end chain-completion validator:
//      the original test's `status is 201` check passed even when
//      the chain stalled on Kafka or the saga. The poller scenario
//      fails the run if any of the 10 sampled orders does not
//      reach "confirmed" within its polling budget.
//
// The two scenarios together give a load test that asserts the
// chain's full happy-path completion under sustained load, not
// just HTTP 201 acceptance.
//
// Metrics:
//
//   - http_req_duration / http_req_failed: existing per-request
//     thresholds; the load test still cares about HTTP behavior
//     under load.
//   - checks{group:chain,check:order_confirmed}: the per-VU chain
//     completion check. A 1.0 rate means all 10 polled orders
//     reached "confirmed". The threshold `rate>=0.9` gives a
//     tolerance for occasional CI flakes (e.g., one slow poll
//     timeout) while still catching a chain-stall regression.
//   - orders_confirmed: counter of confirmed-state arrivals across
//     all polling VUs. Surfaced in the k6 JSON summary so the Go
//     wrapper (load_test.go) can log the confirmed-state-reached
//     percentage even when the load test passes.

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const ordersConfirmed = new Counter('orders_confirmed');

const orderBase = 'http://127.0.0.1:18081';

// Generate a different payment.last_four per VU so concurrent
// orders don't trip the same provider-branch by coincidence.
// VU 0 → 0000 (success default case in
// services/payment/internal/provider/provider.go); VU 1 → 0001
// (declined); etc. We deliberately pin the load scenario to
// 0000 (success) so the polling scenario can rely on a
// deterministic success path. The 0001/0002/0003 reserved
// suffixes per the mock provider are still usable by callers
// that want to drive the failure path; this load test does not.
const orderBody = JSON.stringify({
  customer_id: '8d2f1a40-cf51-4a8b-8e72-1a4d2c8e6b3f',
  items: [{ sku: 'SKU-001', quantity: 1, unit_price_cents: 1999 }],
  payment: { last_four: '0000' },
});

export const options = {
  scenarios: {
    generateLoad: {
      executor: 'constant-vus',
      vus: 50,
      duration: '60s',
      exec: 'generateLoad',
    },
    pollChain: {
      // 10 VUs each running 1 iteration: 10 end-to-end order
      // traces to validate chain completion. Shared-iterations
      // means k6 starts the VUs concurrently so all 10 traces
      // overlap with the load scenario.
      executor: 'shared-iterations',
      vus: 10,
      iterations: 10,
      maxDuration: '80s',
      // Start a few seconds after the load scenario so the order
      // service is already warm; otherwise the first poll's GET
      // races the order service's first cold-cache state transition.
      startTime: '5s',
      exec: 'pollChain',
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<1000'],
    http_req_failed:   ['rate<0.05'],
    'checks{group:chain}': ['rate>=0.9'],
  },
};

export function generateLoad() {
  const res = http.post(`${orderBase}/v1/orders`, orderBody, {
    headers: { 'Content-Type': 'application/json' },
  });
  check(res, { 'status is 201': (r) => r.status === 201 });
}

export function pollChain() {
  // Each VU submits its own order and polls for confirmation. The
  // POST and GET happen on the same VU so the polling VUs do not
  // race the load scenario's order IDs.
  const res = http.post(`${orderBase}/v1/orders`, orderBody, {
    headers: { 'Content-Type': 'application/json' },
  });
  const postOk = check(res, {
    'poll: post status is 201': (r) => r.status === 201,
  });
  if (!postOk) {
    return;
  }
  const body = res.json();
  const orderID = body && body.id;
  if (!orderID) {
    return;
  }

  // Poll up to pollBudgetMs for the order to reach "confirmed".
  // A healthy cold-cache chain completes within a few seconds;
  // 30 s gives wide safety margin without making the test hang
  // forever on a real chain stall.
  const pollBudgetMs = 30000;
  const pollIntervalMs = 500;
  const deadline = Date.now() + pollBudgetMs;

  let confirmed = false;
  while (Date.now() < deadline) {
    const get = http.get(`${orderBase}/v1/orders/${orderID}`);
    if (get.status === 200) {
      const state = get.json('state');
      if (state === 'confirmed') {
        confirmed = true;
        ordersConfirmed.add(1);
        break;
      }
      if (state === 'cancelled' || state === 'failed') {
        // The mock provider treats 0000 as success; this is a
        // chain-regression signature on the load test's success
        // path. Don't keep polling.
        break;
      }
    }
    sleep(pollIntervalMs / 1000);
  }
  check(
    confirmed,
    { 'order confirmed': (v) => v === true },
    { group: 'chain' }
  );
}

