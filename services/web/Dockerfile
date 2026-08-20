FROM golang:1.25.13-alpine AS build
WORKDIR /src

# Build cmd/web from the workspace root. We COPY the entire repo into
# the build context so the go.work workspace resolves every transitive
# module (.replace directives in services/web and cmd/web point at
# sibling workspace modules — they need to be physically present).
COPY . .

WORKDIR /src/cmd/web
RUN go mod download
ARG VERSION=0.0.0-dev
RUN go build -ldflags="-s -w -X github.com/t0pm1x/orderflow/services/web/internal/web.Version=${VERSION}" -o /out/web .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates curl
COPY --from=build /out/web /usr/local/bin/web
# Web's default HTTP_ADDR is :8085 (matches services/web/README.md and
# the launcher scripts/run.* $ports map). Pre-v1.2 HEALTHCHECK pointed
# at :8083, which was the bare-binary default before the v1.1.5
# port-conflict fix that shifted web to :8085 to avoid colliding with
# inventory. docker compose + `docker run` then always marked the
# container unhealthy.
EXPOSE 8085
USER nobody:nobody
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -fsS http://localhost:8085/healthz || exit 1
ENTRYPOINT ["/usr/local/bin/web"]
