# The kaas binary, for running the operator (and the console) in a cluster.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /kaas ./cmd/kaas

FROM gcr.io/distroless/static:nonroot
COPY --from=build /kaas /kaas
# Numeric, so runAsNonRoot can check it: distroless nonroot is 65532.
USER 65532:65532
ENTRYPOINT ["/kaas"]
