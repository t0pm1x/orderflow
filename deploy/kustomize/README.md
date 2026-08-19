# orderflow Kustomize Overlays

Environment-specific configuration on top of a hand-rolled base
(`base/services.yaml`). The overlays were originally written to
patch a `helm template`-rendered bundle (see "Regenerate the base
bundle" below); without `helm` on the controller's PATH,
`base/services.yaml` is a stand-in that mirrors the rendered shape.

## Layout
- `base/` — `namespace.yaml` + `services.yaml` (the 4 service
  Deployments; see the comment at the top of `services.yaml` for
  how to regenerate).
- `overlays/dev/` — dev: replicas=1, reduced resources, ingress.
- `overlays/staging/` — staging: replicas=2.
- `overlays/prod/` — prod: replicas=3, HPA, PDB, larger resources.

The patches under `overlays/*/` strategic-merge against the
Deployment names `orderflow-{order,payment,inventory,saga}` produced
by the base; the overlay's `namePrefix` (where set) prepends the
prefix AFTER the patches are evaluated. When using `kubectl
kustomize`, the patch target name MUST match the (post-prefix) base
name.

## Regenerate the base bundle

    for svc in order payment inventory saga; do
      helm template orderflow-$svc deploy/helm/orderflow-$svc \
        --include-crds \
        >> deploy/kustomize/base/services.yaml
    done

After regenerating, validate:

    kustomize build deploy/kustomize/overlays/dev

## Apply an overlay (with helm on PATH)

    kustomize build deploy/kustomize/overlays/dev   | kubectl apply -f -
    kustomize build deploy/kustomize/overlays/prod  | kubectl apply -f -

`kubectl kustomize` (without standalone `kustomize`) supports the
subset used here; if a feature is missing, install `kustomize`
≥5.0 (`brew install kustomize` / `choco install kustomize`).

## Validation

    kustomize build deploy/kustomize/overlays/dev | kubeconform -strict -summary

## Status of base services.yaml

Kept in sync with `deploy/helm/orderflow-*/values.yaml` as of v1.1.x.
Field-by-field diff against a clean `helm template` output should be
empty; any drift is a bug in this repo and must be fixed by
regenerating the file via the snippet above.
