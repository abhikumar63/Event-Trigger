package main

import (
	"encoding/json"
	"log/slog"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Rule struct {
	ProductID     string `json:"productID"`
	Threshold     int    `json:"threshold"`
	WindowSeconds int    `json:"windowSeconds"`
	TTLSeconds    int    `json:"ttlSeconds"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	bootstrapServers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	if bootstrapServers == "" {
		bootstrapServers = "localhost:9092"
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})
	if err != nil {
		slog.Error("Failed to create producer", "error", err)
		os.Exit(1)
	}
	defer p.Close()

	// Wait for Kafka to be ready
	time.Sleep(10 * time.Second) // Wait for docker-compose to start Kafka properly

	// Delivery report handler
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					slog.Error("Delivery failed", "topic", ev.TopicPartition.Topic, "error", ev.TopicPartition.Error)
				} else {
					slog.Info("Delivered rule", "topic", *ev.TopicPartition.Topic, "partition", ev.TopicPartition.Partition)
				}
			}
		}
	}()

	rulesData, err := os.ReadFile("rules.json")
	if err != nil {
		slog.Error("Failed to read rules.json", "error", err)
		os.Exit(1)
	}

	var rules []Rule
	if err := json.Unmarshal(rulesData, &rules); err != nil {
		slog.Error("Failed to unmarshal rules", "error", err)
		os.Exit(1)
	}

	topic := "rule-stream-topic"
	for {
		for _, rule := range rules {
			payload, _ := json.Marshal(rule)
			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          payload,
			}, nil)
			if err != nil {
				slog.Error("Produce failed", "error", err, "productID", rule.ProductID)
			} else {
				slog.Debug("Enqueued rule for delivery", "productID", rule.ProductID, "threshold", rule.Threshold)
			}
		}

		// Wait for message deliveries before shutting down
		p.Flush(15 * 1000)
		slog.Info("Published rules to Kafka. Sleeping before republishing (just in case)...")
		time.Sleep(30 * time.Second) // Publish every 30s to simulate dynamic updates
	}
}
