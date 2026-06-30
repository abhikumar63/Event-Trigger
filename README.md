# Event-Triggering System

## 1. Project Overview & Philosophy

The **Event-Triggering System** is a high-performance, real-time alert engine designed to process massive streams of transactional data, calculate rolling aggregations, and instantly trigger actionable insights. Built for production-grade reliability, it dynamically evaluates high-frequency traffic against configurable thresholds, ensuring that downstream services are instantly notified of critical business events—such as trending products or sudden volume spikes.

At its core, the architecture is driven by two foundational engineering principles:

- **Zero Trust Data**: Never trust upstream payloads. The system assumes that incoming data may be malformed, corrupted, or structurally mutated. By enforcing strict binary contracts and intercepting serialization failures before they reach the core processing logic, the engine protects itself from cascading failures.
- **Defense-in-Depth**: Resilience is layered at every boundary. From stateful stream isolation and dead-letter queues to strict container health checks and robust telemetry, every component is designed to expect failure and gracefully degrade or recover without halting the primary event loop.

---

## 2. System Architecture & Data Flow

The platform relies on a decoupled, stream-processing topology to ingest, calculate, and externalize state in real-time.

**End-to-End Topology:**

1. **Go Producers (Data Ingestion)**: High-throughput microservices (`order-producer` and `rule-producer`) written in Go act as the edge ingestion layer, generating continuous streams of orders and dynamic rule configurations.
2. **Kafka Brokers (Message Backbone)**: Apache Kafka serves as the durable, distributed message log. It buffers incoming events across highly available partitions (`raw-orders-topic`, `rule-stream-topic`), decoupling ingestion speed from processing speed.
3. **Apache Flink (Compute Core)**: The stateful stream processing engine consumes the Kafka topics. It broadcasts the rule stream across parallel instances and joins it with the keyed order stream. It computes dynamic rolling-window aggregations to detect threshold breaches.
4. **Redis (State Externalization)**: When Flink detects a breach, it fires an alert event. A Go-based `redis-consumer` translates these alerts into high-speed Redis Key-Value pairs with precise Time-To-Live (TTL) expiries. Redis acts as the low-latency serving layer for downstream web or mobile clients to read the active triggers.
5. **PLG Stack (Observability)**: Promtail, Loki, and Grafana provide centralized logging and monitoring, capturing JSON-structured logs from all containers to enable rapid root-cause analysis.

**Normal Operations Flow**:
Orders flow into Kafka as Avro-serialized binary packets. Flink deserializes them using Confluent Schema Registry, partitions the stream by `productID`, and routes them into a 5-minute tumbling/sliding memory window. If the total quantity inside that window breaches a dynamically broadcast threshold, Flink emits an alert to a triggered topic. The Redis consumer picks this up and applies a `SETEX` command, making the alert instantly visible to the outside world. When the traffic cools down and items drop out of the Flink window, Flink stops refreshing the alert, and the Redis key naturally expires.

---

## 3. Core Architectural Decisions Ledger

### Apache Avro & Confluent Schema Registry
- **What it is**: A binary serialization format coupled with a centralized schema governance server.
- **Why it was chosen**: JSON is human-readable but bloated and prone to silent structural drift. Avro enforces a strict schema contract between the Go producers and the Flink consumers while massively compressing the payload.
- **Failure Modes Eliminated**: Eradicates CPU parsing bottlenecks, drastically reduces network bandwidth overhead, and prevents structural drift from rogue upstream publishers that might randomly alter field names or types.

### Flink Side Outputs & Dead Letter Queue (DLQ)
- **What it is**: A defensive boundary within the Flink job that isolates malformed records. The raw bytes are caught in a `try-catch` block during Avro deserialization; if parsing fails, the raw garbage bytes are base64-encoded and safely shunted to a `dlq-orders-topic` via a Flink Side Output.
- **Why it was chosen**: Flink's native deserialization schemas will instantly crash the entire JobManager/TaskManager if a single corrupted byte array is encountered, causing a restart loop.
- **Failure Modes Eliminated**: Completely neutralizes "poison pill" messages. Malformed traffic is safely quarantined without disrupting the main high-throughput sliding window aggregations or causing cluster downtime.

### PLG Observability Stack (Promtail, Loki, Grafana)
- **What it is**: A lightweight, highly efficient log aggregation and visualization suite.
- **Why it was chosen**: Distributed systems generate logs across dozens of ephemeral containers. The PLG stack aggregates these logs into a single queryable interface using LogQL.
- **Failure Modes Eliminated**: Prevents operational blindness. By enforcing structured JSON logging and muting internal framework noise (like Flink/Kafka `AdminClientConfig` chatter), engineers have pristine, real-time dashboards to diagnose exact failure points across the topology.

### Redis State Externalization
- **What it is**: An in-memory data structure store used as the downstream state cache.
- **Why it was chosen**: Flink is exceptional at computing state, but it is not designed to serve high-concurrency read requests from end-users. Redis bridges this gap by offering sub-millisecond read latency.
- **Failure Modes Eliminated**: Prevents the stream processing cluster from being DDOSed by downstream read queries. The TTL mechanism natively handles the "cooldown" phase without requiring complex deletion logic from the Flink engine.

---

## 4. Local Development & Orchestration

The entire topology is orchestrated locally using Docker Compose. To guarantee orderly initialization and prevent race conditions (e.g., Flink attempting to read from Kafka before topics exist, or Schema Registry starting before Kafka), the `docker-compose.yml` utilizes strict health checks.

- `service_healthy`: Ensures dependencies (like Zookeeper and Kafka) are fully accepting connections.
- `service_completed_successfully`: Ensures ephemeral setup scripts (like `kafka-setup`) have finished creating the necessary topics before downstream consumers boot.

### Rapid Bring-Up Commands

To compile the Go binaries, build the custom Flink images (which inject necessary Log4j JSON layout plugins for Loki), and launch the stack:

```bash
# Tear down any stale state and volumes
docker compose down -v

# Build and start the entire architecture in detached mode
docker compose up --build -d
```

To monitor the startup process or check the status of specific containers:
```bash
docker compose ps
docker compose logs -f flink-job-submitter
```

---

## 5. Verification & Chaos Testing

The system includes a dedicated `architecture-tester` and intentional chaos engineering within the `order-producer` to validate resilience.

**The Poison Pill Test**:
The `order-producer` is configured to intentionally inject malformed garbage bytes into the `raw-orders-topic` (a 2% chaos probability). 

To verify that the Defensive DLQ architecture is functioning:
1. Navigate to Grafana (Local port `3000`).
2. Open the Explore tab and select the Loki datasource.
3. Run the following LogQL query to isolate the DLQ warnings:
   ```logql
   {container_name="/event-triggering-system-taskmanager-1"} |= "DLQ CAUGHT POISON PILL"
   ```
4. You will see the Flink engine catching the corrupted bytes and safely routing them away, proving that the system remains stable and continuous under adverse conditions.

**End-to-End Validation**:
You can run the architecture tester to simulate traffic spikes and verify the Redis externalization:
```bash
docker compose --profile manual run --rm architecture-tester
```
