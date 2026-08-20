FROM golang:1.25.13-alpine AS build
WORKDIR /src

# Build cmd/saga from the workspace root. We COPY the entire repo into
# the build context so the go.work workspace resolves every transitive
# module (.replace directives in services/saga and cmd/saga point at
# sibling workspace modules — they need to be physically present).
COPY . .

WORKDIR /src/cmd/saga
RUN go mod download
ARG VERSION=0.0.0-dev
RUN CGO_ENABLED=0 go build -ldflags="-s -w -X github.com/t0pm1x/orderflow/services/saga/cmd/saga.Version=${VERSION}" -o /out/saga .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/saga /usr/local/bin/saga
EXPOSE 8084
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/saga"]
