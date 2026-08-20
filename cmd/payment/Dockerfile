FROM golang:1.25.13-alpine AS build
WORKDIR /src

# Build cmd/payment from the workspace root. We COPY the entire repo into
# the build context so the go.work workspace resolves every transitive
# module (.replace directives in services/payment and cmd/payment point at
# sibling workspace modules — they need to be physically present).
COPY . .

WORKDIR /src/cmd/payment
RUN go mod download
ARG VERSION=0.0.0-dev
RUN CGO_ENABLED=0 go build -ldflags="-s -w -X github.com/t0pm1x/orderflow/services/payment/cmd/payment.Version=${VERSION}" -o /out/payment .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/payment /usr/local/bin/payment
EXPOSE 8082
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/payment"]
