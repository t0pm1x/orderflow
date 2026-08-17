# orderflow Kustomize Overlays

Environment-specific patches on top of the Helm-rendered base.

## Layout
- `base/` — common resources (namespace + Helm-rendered manifests).
- `overlays/dev/` — dev: replicas=1, reduced resources, ingress.
- `overlays/staging/` — staging: replicas=2.
- `overlays/prod/` — prod: replicas=3, HPA, PDB, larger resources.

When Helm is unavailable, `base/all-services.yaml` remains an instructions-only stub and is omitted from `base/kustomization.yaml`. After regenerating the bundle, add `all-services.yaml` to the base `resources` list.

## Regenerate the base bundle

    for svc in order payment inventory saga; do
      helm template orderflow-$svc deploy/helm/orderflow-$svc \
        --include-crds \
        >> deploy/kustomize/base/all-services.yaml
    done

## Apply an overlay

    kustomize build deploy/kustomize/overlays/dev | kubectl apply -f -
    kustomize build deploy/kustomize/overlays/staging | kubectl apply -f -
    kustomize build deploy/kustomize/overlays/prod | kubectl apply -f -

## Validation

    kustomize build deploy/kustomize/overlays/dev | kubeconform -strict -summary
