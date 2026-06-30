package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	confluentavro "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
	"github.com/redis/go-redis/v9"
	avro "architecture-tester/models"
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

	kafkaBrokers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9092"
	}
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "localhost:6379"
	}

	slog.Info("Starting Architecture Tester")

	// 1. Rule Injection Phase
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaBrokers})
	if err != nil {
		slog.Error("Failed to create Kafka producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	// Wait for Kafka and Redis to be ready
	time.Sleep(15 * time.Second)

	slog.Info("[PHASE_START] Rule Injection Phase", "topic", "rule-stream-topic")
	
	rule := Rule{ProductID: "005", Threshold: 200, WindowSeconds: 300, TTLSeconds: 300}
	ruleBytes, _ := json.Marshal(rule)
	topicRule := "rule-stream-topic"
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topicRule, Partition: kafka.PartitionAny},
		Value:          ruleBytes,
	}, nil)
	if err != nil {
		slog.Error("Failed to inject rule", "error", err)
	}
	
	// Also inject rule for 006 for low frequency test
	rule6 := Rule{ProductID: "006", Threshold: 200, WindowSeconds: 300, TTLSeconds: 300}
	ruleBytes6, _ := json.Marshal(rule6)
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topicRule, Partition: kafka.PartitionAny},
		Value:          ruleBytes6,
	}, nil)

	producer.Flush(2000)

	// Schema Registry Setup
	schemaRegistryURL := os.Getenv("SCHEMA_REGISTRY_URL")
	if schemaRegistryURL == "" {
		schemaRegistryURL = "http://schema-registry:8081"
	}

	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistryURL))
	if err != nil {
		slog.Error("Failed to create schema registry client", "error", err)
		os.Exit(1)
	}

	serConfig := confluentavro.NewSerializerConfig()
	serConfig.AutoRegisterSchemas = true
	ser, err := confluentavro.NewSpecificSerializer(srClient, serde.ValueSerde, serConfig)
	if err != nil {
		slog.Error("Failed to create Avro serializer", "error", err)
		os.Exit(1)
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisURL})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	// Active Redis Validator Routine for 005
	wg.Add(1)
	go redisValidator(ctx, &wg, rdb, "005", true)

	// Active Redis Validator Routine for 006 (expect NOT to trigger)
	wg.Add(1)
	go redisValidator(ctx, &wg, rdb, "006", false)

	topicOrder := "raw-orders-topic"

	// 2. Traffic Scenario Engine
	slog.Info("[PHASE_START] Phase 1: Baseline Noise")
	
	// Start baseline noise
	noiseCtx, noiseCancel := context.WithCancel(ctx)
	wg.Add(1)
	go baselineNoise(noiseCtx, &wg, producer, topicOrder, ser)

	// Wait 10 seconds in baseline
	time.Sleep(10 * time.Second)

	slog.Info("[PHASE_START] Phase 2: The Spike (005) & Low Frequency (006)")
	
	// Start Spike
	spikeCtx, spikeCancel := context.WithCancel(ctx)
	wg.Add(1)
	go theSpike(spikeCtx, &wg, producer, topicOrder, ser)
	
	// Start Low Frequency (Phase 4 embedded here to run concurrently)
	wg.Add(1)
	go lowFrequency(spikeCtx, &wg, producer, topicOrder, ser)

	// Run Spike for 60 seconds
	time.Sleep(60 * time.Second)
	
	slog.Info("[PHASE_START] Phase 3: The Cooldown")
	spikeCancel() // Halt the spike and low freq

	// Wait for Redis validator to finish assertion (approx 5 mins after Phase 3)
	// We wait up to 6 minutes.
	time.Sleep(6 * time.Minute)

	noiseCancel() // stop noise
	cancel()      // stop validator if not finished
	wg.Wait()
	slog.Info("Architecture Test Completed")
}

func baselineNoise(ctx context.Context, wg *sync.WaitGroup, producer *kafka.Producer, topic string, ser *confluentavro.SpecificSerializer) {
	defer wg.Done()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 100 random products, avoiding 005 and 006
			productID := fmt.Sprintf("%03d", rand.Intn(100)+10) 
			emitOrder(producer, topic, productID, ser)
		}
	}
}

func theSpike(ctx context.Context, wg *sync.WaitGroup, producer *kafka.Producer, topic string, ser *confluentavro.SpecificSerializer) {
	defer wg.Done()
	// Exactly 250 orders in 60 seconds -> every 240ms
	ticker := time.NewTicker(240 * time.Millisecond)
	defer ticker.Stop()
	count := 0
	for {
		select {
		case <-ctx.Done():
			slog.Info("Spike halted", "orders_sent", count)
			return
		case <-ticker.C:
			if count >= 250 {
				continue 
			}
			emitOrder(producer, topic, "005", ser)
			count++
		}
	}
}

func lowFrequency(ctx context.Context, wg *sync.WaitGroup, producer *kafka.Producer, topic string, ser *confluentavro.SpecificSerializer) {
	defer wg.Done()
	// Send 50 orders in 60 seconds -> every 1200ms
	// This is well below the 200 threshold
	ticker := time.NewTicker(1200 * time.Millisecond)
	defer ticker.Stop()
	count := 0
	for {
		select {
		case <-ctx.Done():
			slog.Info("Low frequency halted", "orders_sent", count)
			return
		case <-ticker.C:
			if count >= 50 {
				continue 
			}
			emitOrder(producer, topic, "006", ser)
			count++
		}
	}
}

func emitOrder(producer *kafka.Producer, topic, productID string, ser *confluentavro.SpecificSerializer) {
	event := avro.NewOrderEvent()
	event.OrderId = fmt.Sprintf("ord-%d-%d", time.Now().UnixNano(), rand.Intn(1000))
	event.Timestamp = time.Now().UnixMilli()
	event.Items = []avro.Item{{ProductId: productID, Quantity: 1}}

	payload, err := ser.Serialize(topic, &event)
	if err != nil {
		slog.Error("Serialization failed", "error", err)
		return
	}

	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          payload,
	}, nil)
	if err == nil {
		slog.Debug("[METRIC_EMITTED]", "productID", productID)
	}
}

func redisValidator(ctx context.Context, wg *sync.WaitGroup, rdb *redis.Client, productID string, expectTrigger bool) {
	defer wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	key := "trending:product:" + productID
	seen := false

	for {
		select {
		case <-ctx.Done():
			if !expectTrigger && !seen {
				slog.Info("[REDIS_ASSERTION_PASSED] Key never appeared as expected", "key", key)
			} else if expectTrigger && !seen {
				slog.Error("[REDIS_ASSERTION_FAILED] Key never appeared", "key", key)
			}
			return
		case <-ticker.C:
			ttl, err := rdb.TTL(ctx, key).Result()
			if err == nil && ttl > 0 {
				if !seen {
					if expectTrigger {
						slog.Info("[REDIS_ASSERTION_PASSED] Key appeared", "key", key)
					} else {
						slog.Error("[REDIS_ASSERTION_FAILED] Key appeared but was not expected to", "key", key)
					}
					seen = true
				}
				slog.Info("TTL status", "key", key, "ttl", ttl.String())
			} else if err == redis.Nil || ttl <= 0 {
				if seen && expectTrigger {
					slog.Info("[REDIS_ASSERTION_PASSED] Key completely disappeared", "key", key)
					return // Success
				}
			}
		}
	}
}
