FROM golang:latest AS builder
WORKDIR /app
COPY go.mod ./
COPY main.go ./
COPY models/ ./models/
RUN go mod tidy
RUN go build -o architecture-tester main.go

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/architecture-tester .
CMD ["./architecture-tester"]
