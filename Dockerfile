# ABOUTME: Multi-stage build for noseyposey — produces a minimal runtime image.
# ABOUTME: Uses CGo-free sqlite driver so the binary is fully static.
FROM golang:1.26 AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /noseyposey ./cmd/noseyposey

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /noseyposey /noseyposey
ENTRYPOINT ["/noseyposey"]
