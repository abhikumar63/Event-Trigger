package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Item struct {
	ProductID string `json:"productId"`
	Quantity  int    `json:"quantity"`
}

type OrderEvent struct {
	OrderID   string `json:"orderId"`
	Timestamp int64  `json:"timestamp"`
	Items     []Item `json:"items"`
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

	// Wait for Kafka
	time.Sleep(10 * time.Second)

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					slog.Error("Delivery failed", "error", ev.TopicPartition.Error, "topic", *ev.TopicPartition.Topic)
				} else {
					slog.Info("Successfully delivered order event to Kafka", "topic", *ev.TopicPartition.Topic, "partition", ev.TopicPartition.Partition, "offset", ev.TopicPartition.Offset)
				}
			}
		}
	}()

	topic := "raw-orders-topic"
	slog.Info("Starting to produce order events")

	productIDs := []string{"001", "002", "003", "004"}

	for {
		numItems := rand.Intn(5) + 1
		items := make([]Item, numItems)
		hasTargetProduct := false

		if rand.Float32() <= 0.7 {
			hasTargetProduct = true
		}

		for i := 0; i < numItems; i++ {
			pID := productIDs[rand.Intn(len(productIDs))]
			if i == 0 && hasTargetProduct {
				pID = "001"
			}
			items[i] = Item{
				ProductID: pID,
				Quantity:  rand.Intn(10) + 1,
			}
		}

		event := OrderEvent{
			OrderID:   fmt.Sprintf("ord-%d-%d", time.Now().UnixNano(), rand.Intn(1000)),
			Timestamp: time.Now().UnixMilli(),
			Items:     items,
		}

		payload, _ := json.Marshal(event)
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          payload,
		}, nil)

		if err != nil {
			slog.Error("Produce failed", "error", err, "orderID", event.OrderID)
		} else {
			slog.Debug("Enqueued order event for delivery", "orderID", event.OrderID, "itemCount", len(event.Items))
		}

		time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond) // Average ~4 events/sec total
	}
}
