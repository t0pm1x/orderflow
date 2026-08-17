# orderflow ArgoCD Delivery

GitOps delivery via ArgoCD ApplicationSet.

## Apply

    kubectl apply -f deploy/argocd/projects.yaml
    kubectl apply -f deploy/argocd/appset.yaml

## Per-environment Applications

If you prefer individual Applications over the ApplicationSet, see `overlays/{dev,staging,prod}.yaml`.

## Sync policy

- **Automated** with prune + selfHeal (production gets the same — change via `syncPolicy.automated.selfHeal: false` if you want manual prod promotion).
- **Retry**: 5 attempts with exponential backoff (10s → 5m max).
- **Prune**: foreground propagation with `PruneLast=true` so dependents go first.

## Project RBAC

`projects.yaml` constrains destinations to `orderflow-*` namespaces and allows only the resource kinds we actually deploy.