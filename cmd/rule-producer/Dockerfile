FROM golang:latest AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o rule-producer main.go

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/rule-producer .
COPY rules.json .
CMD ["./rule-producer"]
