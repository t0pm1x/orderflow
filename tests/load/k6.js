import http from 'k6/http';
import { check } from 'k6';

export const options = {
  vus: 50,
  duration: '60s',
  thresholds: {
    http_req_duration: ['p(95)<1000'],
    http_req_failed:   ['rate<0.05'],
  },
};

const orderBody = JSON.stringify({
  customer_id: '8d2f1a40-cf51-4a8b-8e72-1a4d2c8e6b3f',
  items: [{ sku: 'SKU-001', quantity: 1, unit_price_cents: 1999 }],
});

export default function () {
  const res = http.post('http://127.0.0.1:18081/v1/orders', orderBody, {
    headers: { 'Content-Type': 'application/json' },
  });
  check(res, { 'status is 201': (r) => r.status === 201 });
}
