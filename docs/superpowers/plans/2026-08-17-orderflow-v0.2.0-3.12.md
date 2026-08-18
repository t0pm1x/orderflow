# Stage 3.12 — Helm + Kustomize + ArgoCD + kind smoke

**Why:** `deploy/{helm,kustomize,argocd}/` are empty. Only namespace/RBAC/netpol exist in `deploy/k8s/base/`. We need reproducible k8s delivery before v0.2.0.

**Depends on:** Stage 3.10 done (chi server in cmd binaries for k8s probes). Stage 3.11 not required (E2E tests work locally via docker-compose; k8s smoke validates deployable manifests separately).

### Task 3.12.a — Helm charts per service (PAR; a.1..a.4 can run in parallel)

**Files (per service):**
- Create: `deploy/helm/orderflow-order/Chart.yaml`
- Create: `deploy/helm/orderflow-order/values.yaml`
- Create: `deploy/helm/orderflow-order/templates/deployment.yaml`
- Create: `deploy/helm/orderflow-order/templates/service.yaml`
- Create: `deploy/helm/orderflow-order/templates/serviceaccount.yaml`
- Create: `deploy/helm/orderflow-order/templates/configmap.yaml`
- Create: `deploy/helm/orderflow-order/templates/_helpers.tpl`

Repeat for `orderflow-payment`, `orderflow-inventory`, `orderflow-saga`.

**Interfaces:**
- `helm install orderflow-order deploy/helm/orderflow-order --values values-prod.yaml`
- All charts depend on `orderflow-postgres`, `orderflow-redis`, `orderflow-redpanda` (Task 3.12.b).

- [ ] **Step 1: Per-service chart scaffold**

For each service, run `helm create deploy/helm/orderflow-<svc>` then strip out `NOTES.txt` and replace generated templates with the patterns below. (Helm CLI is not strictly required if you write the YAML directly; see Step 2.)

- [ ] **Step 2: `Chart.yaml`** (template)

```yaml
apiVersion: v2
name: orderflow-order
description: Order Service for orderflow
type: application
version: 0.1.0
appVersion: "0.2.0"
```

- [ ] **Step 3: `values.yaml`** (template, per service)

```yaml
image:
  repository: ghcr.io/t0pm1x/orderflow-order
  tag: "0.2.0"
  pullPolicy: IfNotPresent
replicas: 2
service:
  port: 8081
env:
  DATABASE_URL: ""
  KAFKA_BROKER: ""
  REDIS_URL: ""
  HTTP_ADDR: ":8081"
resources:
  requests: { cpu: "100m", memory: "128Mi" }
  limits:   { cpu: "500m", memory: "512Mi" }
probes:
  liveness:  /healthz
  readiness: /healthz
```

- [ ] **Step 4: `templates/deployment.yaml`**

Use `helm.sh/helm/v3` template helpers. Container ports, env from values, liveness/readiness probes pointing at `/healthz`, `serviceAccountName: {{ include "orderflow-order.serviceAccountName" . }}`.

- [ ] **Step 5: `templates/service.yaml`**

ClusterIP service on `service.port`.

- [ ] **Step 6: Lint each chart**

```powershell
helm lint deploy/helm/orderflow-order
```
Repeat for the other three. Expect: no errors.

- [ ] **Step 7: Commit per service (4 commits)**

```powershell
git add deploy/helm/orderflow-order
git commit -m "orderflow/3.12.a.order: Helm chart for Order Service"
# repeat for payment, inventory, saga
```

### Task 3.12.b — Infra charts: postgres, redis, redpanda (PAR)

**Files:**
- Create: `deploy/helm/orderflow-postgres/...` (per-service postgres: order, payment, inventory)
- Create: `deploy/helm/orderflow-redis/...` (single)
- Create: `deploy/helm/orderflow-redpanda/...` (single)

**Interfaces:**
- Bitnami charts preferred (`oci://registry-1.docker.io/bitnamicharts/postgresql`, `.../redis`, `.../kafka`). Use them as dependencies.

- [ ] **Step 1: per-service postgres chart**

`deploy/helm/orderflow-postgres/Chart.yaml`:
```yaml
apiVersion: v2
name: orderflow-postgres
dependencies:
  - name: postgresql
    version: 16.x.x
    repository: oci://registry-1.docker.io/bitnamicharts
```
`values.yaml` declares three databases (`order_order`, `payment_payment`, `inventory_inventory`) via `postgresql.multipleDatabases` (Bitnami 16 supports this via `postgresql.databases` list). Init scripts mounted via `existingConfigMap` from a sibling configmap per service.

- [ ] **Step 2: redis chart**

`deploy/helm/orderflow-redis/Chart.yaml` deps on `oci://.../redis`, single instance.

- [ ] **Step 3: redpanda chart**

`deploy/helm/orderflow-redpanda/Chart.yaml` deps on `oci://.../redpanda` (Bitnami has it). Single broker for dev; values override `replicas: 1`.

- [ ] **Step 4: `helm dependency update` + lint**

```powershell
cd C:\Users\t0p_m\projects\orderflow\deploy\helm\orderflow-postgres
helm dependency update
helm lint .
```
Repeat.

- [ ] **Step 5: Commit**

```powershell
git add deploy/helm/orderflow-postgres deploy/helm/orderflow-redis deploy/helm/orderflow-redpanda
git commit -m "orderflow/3.12.b: infra Helm charts (postgres/redis/redpanda)"
```

### Task 3.12.c — Kustomize overlays (PAR with 3.12.d)

**Files:**
- Create: `deploy/kustomize/base/kustomization.yaml`
- Create: `deploy/kustomize/overlays/dev/kustomization.yaml`
- Create: `deploy/kustomize/overlays/staging/kustomization.yaml`
- Create: `deploy/kustomize/overlays/prod/kustomization.yaml`
- Create: `deploy/kustomize/overlays/dev/replicas.yaml` patch

- [ ] **Step 1: Base**

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../../k8s/base/namespace.yaml
  - ../../k8s/base/rbac.yaml
  - ../../k8s/base/network-policies.yaml
```
For each service, run `helm template orderflow-order deploy/helm/orderflow-order > base/order.yaml` and add it to `resources`.

- [ ] **Step 2: dev overlay**

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: orderflow-dev
resources:
  - ../base
namePrefix: dev-
patches:
  - path: replicas.yaml
```
`replicas.yaml`:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata: { name: dev-order }
spec: { replicas: 1 }
```

- [ ] **Step 3: staging/prod**

Same shape, different namespace + replicas.

- [ ] **Step 4: Validate**

```powershell
kustomize build deploy/kustomize/overlays/dev | kubeconform -strict -summary
```

- [ ] **Step 5: Commit**

```powershell
git add deploy/kustomize
git commit -m "orderflow/3.12.c: Kustomize overlays (dev/staging/prod)"
```

### Task 3.12.d — ArgoCD Application manifests (PAR with 3.12.c)

**Files:**
- Create: `deploy/argocd/apps/order.yaml`
- Create: `deploy/argocd/apps/payment.yaml`
- Create: `deploy/argocd/apps/inventory.yaml`
- Create: `deploy/argocd/apps/saga.yaml`
- Create: `deploy/argocd/appset.yaml` (ApplicationSet that fans out from a directory)

**Interfaces:**
- ArgoCD discovers apps via `argocd-appset.yaml`. Each `Application` points at `deploy/kustomize/overlays/<env>` with `repoURL: https://github.com/t0pm1x/orderflow.git`.

- [ ] **Step 1: App per service**

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata: { name: orderflow-order-dev, namespace: argocd }
spec:
  project: default
  source:
    repoURL: https://github.com/t0pm1x/orderflow.git
    targetRevision: main
    path: deploy/kustomize/overlays/dev
  destination: { server: https://kubernetes.default.svc, namespace: orderflow-dev }
  syncPolicy: { automated: { prune: true, selfHeal: true } }
```

- [ ] **Step 2: ApplicationSet**

```yaml
apiVersion: argoproj.io/v1alpha1
kind: ApplicationSet
metadata: { name: orderflow, namespace: argocd }
spec:
  generators:
    - list: { elements: [{name: order}, {name: payment}, {name: inventory}, {name: saga}] }
  template:
    metadata: { name: 'orderflow-{{name}}-dev' }
    spec:
      source:
        repoURL: https://github.com/t0pm1x/orderflow.git
        path: deploy/kustomize/overlays/dev
```

- [ ] **Step 3: Validate**

```powershell
kubectl --dry-run=server apply -f deploy/argocd/appset.yaml
```

- [ ] **Step 4: Commit**

```powershell
git add deploy/argocd
git commit -m "orderflow/3.12.d: ArgoCD Application manifests for GitOps delivery"
```

### Task 3.12.e — kind cluster config (PAR; independent)

**Files:**
- Create: `deploy/kind/kind.yaml`
- Modify: `Makefile` (add `kind-up`, `kind-down`, `kind-load`)

**Interfaces:**
- `make kind-up` creates a kind cluster with port mappings matching docker-compose (8081-8083, 9092, 3000).

- [ ] **Step 1: kind.yaml**

```yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraPortMappings:
      - { containerPort: 30080, hostPort: 8081 }
      - { containerPort: 30081, hostPort: 8082 }
      - { containerPort: 30082, hostPort: 8083 }
      - { containerPort: 30090, hostPort: 9092 }
      - { containerPort: 30300, hostPort: 3000 }
```

- [ ] **Step 2: Makefile targets**

```make
KIND        := kind
KIND_CLUSTER := orderflow
KIND_IMAGE  := kindest/node:v1.30.0

kind-up:
	$(KIND) create cluster --name $(KIND_CLUSTER) --config deploy/kind/kind.yaml --image $(KIND_IMAGE)

kind-down:
	$(KIND) delete cluster --name $(KIND_CLUSTER)

kind-load:
	$(KIND) load docker-image --name $(KIND_CLUSTER) ghcr.io/t0pm1x/orderflow-order:dev
```

- [ ] **Step 3: Document prerequisite**

Append to README "Local development" section: "Requires `kind` v0.23+ — install with `winget install Kubernetes.kind` (Windows) or `brew install kind` (macOS)."

- [ ] **Step 4: Commit**

```powershell
git add deploy/kind Makefile README.md
git commit -m "orderflow/3.12.e: kind cluster config + make kind-up/down"
```

### Task 3.12.f — kind smoke test (SEQ; depends on a..e)

**Files:**
- Create: `tests/k8s/smoke_test.go`
- Modify: `Makefile` (add `make smoke`)

**Interfaces:**
- Spins up kind cluster, installs infra + service charts, hits `/healthz` on each, runs happy-path curl, tears down.

- [ ] **Step 1: Smoke test**

```go
func TestK8sSmoke_AllServicesHealthy(t *testing.T) {
    if testing.Short() { t.Skip() }
    if os.Getenv("KUBECONFIG") == "" { t.Skip("needs kind cluster") }
    // exec.Command("helm", "install", "infra", "deploy/helm/orderflow-postgres", "--wait")
    // exec.Command("helm", "install", "orderflow-order-dev", "deploy/helm/orderflow-order")
    // repeat for payment, inventory, saga
    // kubectl wait --for=condition=Ready pod -l app=orderflow-order --timeout=120s
    // curl http://localhost:8081/healthz -> 200
}
```

- [ ] **Step 2: Makefile target**

```make
smoke:
	go test ./tests/k8s/... -v -timeout 15m
```

- [ ] **Step 3: Commit**

```powershell
git add tests/k8s Makefile
git commit -m "orderflow/3.12.f: kind smoke test"
```