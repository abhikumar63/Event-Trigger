package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/redis/go-redis/v9"
)

type AlertTriggered struct {
	ProductID  string `json:"productID"`
	TTLSeconds int    `json:"ttlSeconds"`
	Count      int    `json:"count"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	bootstrapServers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	if bootstrapServers == "" {
		bootstrapServers = "localhost:9092"
	}

	redisUrl := os.Getenv("REDIS_URL")
	if redisUrl == "" {
		redisUrl = "localhost:6379"
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: redisUrl,
	})
	ctx := context.Background()

	// Verify Redis connection
	if err := rdb.Ping(ctx).Err(); err != nil {
		slog.Error("Failed to connect to Redis", "error", err)
		os.Exit(1)
	}
	slog.Info("Connected to Redis", "addr", redisUrl)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          "redis-consumer-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		slog.Error("Failed to create consumer", "error", err)
		os.Exit(1)
	}

	topic := "triggered-alerts-topic"
	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		slog.Error("Failed to subscribe", "error", err)
		os.Exit(1)
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	slog.Info("Starting Redis consumer")
	run := true
	for run {
		select {
		case sig := <-sigchan:
			slog.Info("Caught signal, terminating", "signal", sig)
			run = false
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				slog.Info("Received alert message from Kafka", "topic", *e.TopicPartition.Topic, "partition", e.TopicPartition.Partition, "offset", e.TopicPartition.Offset)
				var alert AlertTriggered
				if err := json.Unmarshal(e.Value, &alert); err != nil {
					slog.Error("Failed to unmarshal alert", "error", err)
					continue
				}

				key := fmt.Sprintf("trending:product:%s", alert.ProductID)
				err := rdb.SetEx(ctx, key, "true", time.Duration(alert.TTLSeconds)*time.Second).Err()
				if err != nil {
					slog.Error("Failed to update Redis", "key", key, "error", err)
				} else {
					slog.Info("Updated trending status in Redis", "productID", alert.ProductID, "ttlSeconds", alert.TTLSeconds)
				}
			case kafka.Error:
				slog.Error("Kafka error", "error", e)
			}
		}
	}

	slog.Info("Closing consumer")
	_ = c.Close()
	_ = rdb.Close()
}
