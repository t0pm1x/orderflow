FROM golang:1.25.13-alpine AS build
WORKDIR /src
COPY go.work go.work.sum ./
COPY pkg ./pkg
COPY services/web ./services/web
WORKDIR /src/services/web
RUN go mod download
ARG VERSION=0.0.0-dev
RUN go build -ldflags="-s -w -X github.com/t0pm1x/orderflow/services/web/internal/web.Version=${VERSION}" -o /out/web ./cmd/web

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /out/web /usr/local/bin/web
EXPOSE 8083
USER nobody:nobody
ENTRYPOINT ["/usr/local/bin/web"]
