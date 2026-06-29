FROM golang:latest AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o order-producer main.go

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/order-producer .
CMD ["./order-producer"]
