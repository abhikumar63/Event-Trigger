FROM golang:latest AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o redis-consumer main.go

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/redis-consumer .
CMD ["./redis-consumer"]
