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
EXPOSE 8083
USER nobody:nobody
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -fsS http://localhost:8083/healthz || exit 1
ENTRYPOINT ["/usr/local/bin/web"]
