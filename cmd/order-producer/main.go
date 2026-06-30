package main

import (
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"time"

	models "order-producer/models"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	confluentavro "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
)

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

	// Schema Registry Setup
	schemaRegistryURL := os.Getenv("SCHEMA_REGISTRY_URL")
	if schemaRegistryURL == "" {
		schemaRegistryURL = "http://localhost:8081"
	}

	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistryURL))
	if err != nil {
		slog.Error("Failed to create schema registry client", "error", err)
		os.Exit(1)
	}

	// Read and Register Schema
	schemaBytes, err := os.ReadFile("/schema/OrderEvent.avsc")
	if err != nil {
		slog.Error("Failed to read schema file", "error", err)
		os.Exit(1)
	}

	schemaInfo := schemaregistry.SchemaInfo{
		Schema:     string(schemaBytes),
		SchemaType: "AVRO",
	}
	schemaID, err := srClient.Register(topic+"-value", schemaInfo, false)
	if err != nil {
		slog.Error("Failed to register schema", "error", err)
		os.Exit(1)
	}
	slog.Info("Schema registered successfully", "schemaID", schemaID)

	ser, err := confluentavro.NewSpecificSerializer(srClient, serde.ValueSerde, confluentavro.NewSerializerConfig())
	if err != nil {
		slog.Error("Failed to create Avro serializer", "error", err)
		os.Exit(1)
	}

	slog.Info("Starting to produce order events")

	productIDs := []string{"001", "002", "003", "004"}

	for {
		numItems := rand.Intn(5) + 1
		items := make([]models.Item, numItems)
		hasTargetProduct := false

		if rand.Float32() <= 0.7 {
			hasTargetProduct = true
		}

		for i := 0; i < numItems; i++ {
			pID := productIDs[rand.Intn(len(productIDs))]
			if i == 0 && hasTargetProduct {
				pID = "001"
			}
			items[i] = models.Item{
				ProductId: pID,
				Quantity:  int32(rand.Intn(10) + 1),
			}
		}

		event := models.NewOrderEvent()
		event.OrderId = fmt.Sprintf("ord-%d-%d", time.Now().UnixNano(), rand.Intn(1000))
		event.Timestamp = time.Now().UnixMilli()
		event.Items = items

		var payload []byte
		if rand.Float32() < 0.02 { // 2% Poison Pill Chance
			payload = []byte(fmt.Sprintf("{\"broken_json_garbage_poison_pill_%d...", rand.Intn(1000)))
			slog.Warn("[POISON PILL] Injecting malformed garbage into raw-orders-topic")
		} else {
			payload, err = ser.Serialize(topic, &event)
			if err != nil {
				slog.Error("Failed to serialize OrderEvent to Avro", "error", err)
				continue
			}
		}

		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          payload,
		}, nil)

		if err != nil {
			slog.Error("Produce failed", "error", err, "orderID", event.OrderId)
		} else {
			if payload[0] == 0 { // Valid Avro magic byte
				slog.Debug("Enqueued Avro order event for delivery", "orderID", event.OrderId, "itemCount", len(event.Items))
			}
		}

		time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond) // Average ~4 events/sec total
	}
}
