FROM golang:1.25.13-alpine AS build
WORKDIR /src

# Build cmd/inventory from the workspace root. We COPY the entire repo into
# the build context so the go.work workspace resolves every transitive
# module (.replace directives in services/inventory and cmd/inventory point at
# sibling workspace modules — they need to be physically present).
COPY . .

WORKDIR /src/cmd/inventory
RUN go mod download
ARG VERSION=0.0.0-dev
RUN CGO_ENABLED=0 go build -ldflags="-s -w -X github.com/t0pm1x/orderflow/services/inventory/cmd/inventory.Version=${VERSION}" -o /out/inventory .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/inventory /usr/local/bin/inventory
EXPOSE 8083
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/inventory"]
