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
	"github.com/redis/go-redis/v9"
)

type Rule struct {
	ProductID     string `json:"productID"`
	Threshold     int    `json:"threshold"`
	WindowSeconds int    `json:"windowSeconds"`
	TTLSeconds    int    `json:"ttlSeconds"`
}

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
	time.Sleep(10 * time.Second)

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
	producer.Flush(2000)

	rdb := redis.NewClient(&redis.Options{Addr: redisURL})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	// Active Redis Validator Routine
	wg.Add(1)
	go redisValidator(ctx, &wg, rdb)

	topicOrder := "raw-orders-topic"

	// 2. Traffic Scenario Engine
	slog.Info("[PHASE_START] Phase 1: Baseline Noise")
	
	// Start baseline noise
	noiseCtx, noiseCancel := context.WithCancel(ctx)
	wg.Add(1)
	go baselineNoise(noiseCtx, &wg, producer, topicOrder)

	// Wait 10 seconds in baseline
	time.Sleep(10 * time.Second)

	slog.Info("[PHASE_START] Phase 2: The Spike")
	
	// Start Spike
	spikeCtx, spikeCancel := context.WithCancel(ctx)
	wg.Add(1)
	go theSpike(spikeCtx, &wg, producer, topicOrder)

	// Run Spike for 60 seconds
	time.Sleep(60 * time.Second)
	
	slog.Info("[PHASE_START] Phase 3: The Cooldown")
	spikeCancel() // Halt the spike

	// Wait for Redis validator to finish assertion (approx 5 mins after Phase 3)
	// We wait up to 6 minutes.
	time.Sleep(6 * time.Minute)

	noiseCancel() // stop noise
	cancel()      // stop validator if not finished
	wg.Wait()
	slog.Info("Architecture Test Completed")
}

func baselineNoise(ctx context.Context, wg *sync.WaitGroup, producer *kafka.Producer, topic string) {
	defer wg.Done()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 100 random products, avoiding 001
			productID := fmt.Sprintf("%03d", rand.Intn(100)+2) 
			emitOrder(producer, topic, productID)
		}
	}
}

func theSpike(ctx context.Context, wg *sync.WaitGroup, producer *kafka.Producer, topic string) {
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
			emitOrder(producer, topic, "005")
			count++
		}
	}
}

func emitOrder(producer *kafka.Producer, topic, productID string) {
	event := OrderEvent{
		OrderID:   fmt.Sprintf("ord-%d-%d", time.Now().UnixNano(), rand.Intn(1000)),
		Timestamp: time.Now().UnixMilli(),
		Items:     []Item{{ProductID: productID, Quantity: 1}},
	}
	payload, _ := json.Marshal(event)
	err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          payload,
	}, nil)
	if err == nil {
		slog.Debug("[METRIC_EMITTED]", "productID", productID)
	}
}

func redisValidator(ctx context.Context, wg *sync.WaitGroup, rdb *redis.Client) {
	defer wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	key := "trending:product:005"
	seen := false

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ttl, err := rdb.TTL(ctx, key).Result()
			if err == nil && ttl > 0 {
				if !seen {
					slog.Info("[REDIS_ASSERTION_PASSED] Key appeared", "key", key)
					seen = true
				}
				slog.Info("TTL status", "key", key, "ttl", ttl.String())
			} else if err == redis.Nil || ttl <= 0 {
				if seen {
					slog.Info("[REDIS_ASSERTION_PASSED] Key completely disappeared", "key", key)
					return // Success
				}
			}
		}
	}
}
