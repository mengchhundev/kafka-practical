# Kafka Documentation
> Spring Boot 3.3 + Kafka 3.7 · KRaft Mode · Docker · Practical Guide

---

## Table of Contents
1. [How Kafka Works](#1-how-kafka-works)
2. [Kafka Configuration](#2-kafka-configuration)
3. [Debug Kafka in Container](#3-debug-kafka-in-container)
4. [Testing Guide](#4-testing-guide)
5. [Production Best Practices](#5-production-best-practices)

---

## 1. How Kafka Works

### 1.1 Core Concepts

```
┌─────────────────────────────────────────────────────────────────┐
│                        KAFKA CLUSTER                            │
│                                                                 │
│  ┌──────────┐    publish     ┌─────────────────────────────┐   │
│  │ Producer │ ─────────────► │           Topic             │   │
│  └──────────┘                │  ┌──────┬──────┬──────┐    │   │
│                              │  │ P-0  │ P-1  │ P-2  │    │   │
│  POST /api/kafka/send        │  │off:0 │off:0 │off:0 │    │   │
│                              │  │off:1 │off:1 │      │    │   │
│                              │  └──────┴──────┴──────┘    │   │
│                              └──────────────┬──────────────┘   │
│                                             │ consume           │
│                              ┌──────────────▼──────────────┐   │
│                              │         Consumer Group       │   │
│                              │  ┌───────────┐              │   │
│                              │  │ Consumer  │ demo-group   │   │
│                              │  └───────────┘              │   │
│                              └─────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### 1.2 Key Components

| Component | Description | In This Project |
|---|---|---|
| **Producer** | Publishes messages to a topic | `MessageProducer.java` |
| **Consumer** | Reads messages from a topic | `MessageConsumer.java` |
| **Topic** | Named channel for messages | `my-topic` |
| **Partition** | Ordered, immutable log within a topic | 3 partitions |
| **Offset** | Unique position of a message in a partition | Auto-tracked |
| **Consumer Group** | Group of consumers sharing partition load | `demo-group`, `demo-group-advanced` |
| **Broker** | Kafka server that stores and serves messages | Docker container on port `9092` |

---

### 1.3 Message Flow (Step by Step)

```
Step 1: Client sends HTTP POST
──────────────────────────────
  curl -X POST "http://localhost:8081/api/kafka/send?message=Hello"
                      │
                      ▼
Step 2: Controller calls Producer
──────────────────────────────────
  MessageController.send()
      └── MessageProducer.send("Hello")
              └── kafkaTemplate.send("my-topic", "Hello")

Step 3: Kafka assigns partition
────────────────────────────────
  No key supplied → Round-robin across partitions
  Key supplied    → hash(key) % numPartitions → deterministic partition

  "Hello" → partition=0, offset=0

Step 4: Broker persists message
────────────────────────────────
  Appended to /var/kafka/data/my-topic-0/
  Retained for 168 hours (log.retention.hours)

Step 5: Consumers receive message
───────────────────────────────────
  demo-group          → MessageConsumer.listen()          → logs value only
  demo-group-advanced → MessageConsumer.listenWithMetadata() → logs full record
```

---

### 1.4 Partitions & Consumer Groups

```
Topic: my-topic (3 partitions)
┌──────────┬──────────┬──────────┐
│  P-0     │  P-1     │  P-2     │
│ msg0     │ msg1     │ msg2     │
│ msg3     │          │          │
└──────────┴──────────┴──────────┘
     │            │          │
     └────────────┴──────────┘
                  │
          Consumer Group "demo-group"
          (1 consumer → gets ALL 3 partitions)

Rule: Each partition is assigned to exactly ONE consumer per group.
      If you add more consumers than partitions → extras sit idle.
```

### 1.5 KRaft Mode (No Zookeeper)

This project uses **KRaft** (Kafka Raft Metadata) — Kafka's built-in consensus mode introduced in Kafka 3.3+ that **eliminates the Zookeeper dependency**.

```
Traditional Kafka:  Broker ←──── Zookeeper (separate process)
KRaft Kafka:        Broker + Controller (same process, no Zookeeper!)
```

The broker here plays both roles:
```properties
process.roles=broker,controller   # dual role
node.id=1
controller.quorum.voters=1@localhost:9093
```

---

## 2. Kafka Configuration

### 2.1 Broker Configuration — `kafka/server.properties`

```properties
# ── ROLES ──────────────────────────────────────────────────────
process.roles=broker,controller     # This node is both broker AND controller
node.id=1                           # Unique ID for this node in the cluster

# ── RAFT CONSENSUS ─────────────────────────────────────────────
controller.quorum.voters=1@localhost:9093
# Format: nodeId@host:port — list ALL controller nodes here
# For multi-node: 1@host1:9093,2@host2:9093,3@host3:9093

# ── LISTENERS ──────────────────────────────────────────────────
listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
# PLAINTEXT → client-facing (producers/consumers connect here)
# CONTROLLER → internal Raft traffic between controller nodes

advertised.listeners=PLAINTEXT://localhost:9092
# What clients use to connect back — MUST be reachable from outside Docker

listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER

# ── STORAGE ────────────────────────────────────────────────────
log.dirs=/var/kafka/data            # Where message data is stored in the container

# ── TOPIC DEFAULTS ─────────────────────────────────────────────
num.partitions=1                    # Default partition count for new topics
default.replication.factor=1       # Safe for single-node dev; use 3 in production
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1

# ── RETENTION ──────────────────────────────────────────────────
log.retention.hours=168             # Keep messages for 7 days
log.segment.bytes=1073741824        # Roll a new segment file at 1 GB
log.retention.check.interval.ms=300000  # Check retention every 5 minutes
```

---

### 2.2 Spring Boot Configuration — `application.yml`

```yaml
server:
  port: 8081

spring:
  kafka:
    bootstrap-servers: localhost:9092   # Kafka broker address

    # ── PRODUCER ───────────────────────────────────────────────
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      # acks: all          # Strongest durability — wait for all replicas
      # retries: 3         # Retry on transient failures
      # batch-size: 16384  # Batch up to 16KB before sending

    # ── CONSUMER ───────────────────────────────────────────────
    consumer:
      group-id: demo-group
      auto-offset-reset: earliest     # Start from beginning if no committed offset
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      # enable-auto-commit: true      # Auto-commit offsets every 5s (default)
      # max-poll-records: 500         # Max records per poll()

app:
  kafka:
    topic: my-topic
```

#### `auto-offset-reset` values explained:

| Value | Behaviour |
|---|---|
| `earliest` | Start from offset 0 — reads all historical messages |
| `latest` | Start from now — only new messages after consumer starts |
| `none` | Throw error if no offset found — strict production use |

---

### 2.3 Topic Configuration — `KafkaConfig.java`

```java
@Bean
public NewTopic myTopic() {
    return TopicBuilder.name("my-topic")
            .partitions(3)    // 3 partitions → up to 3 consumers in parallel
            .replicas(1)      // 1 replica (dev only; use 3 in production)
            .build();
}
```

#### Partition count guidelines:

| Consumers | Partitions | Result |
|---|---|---|
| 1 consumer | 3 partitions | 1 consumer handles all 3 — no parallelism |
| 3 consumers | 3 partitions | 1 partition per consumer — full parallelism ✅ |
| 4 consumers | 3 partitions | 1 consumer idles — wasted resource |

---

### 2.4 Docker Configuration — `docker-compose.yaml` & `Dockerfile`

```yaml
# docker-compose.yaml
services:
  kafka:
    build: ./kafka          # Builds from kafka/Dockerfile
    ports:
      - "9092:9092"         # host:container — expose broker to localhost
    volumes:
      - kafka-data:/var/kafka/data   # Persist messages across container restarts
```

```dockerfile
# kafka/Dockerfile
FROM eclipse-temurin:17-jre-alpine     # Lightweight JRE base image
ARG KAFKA_VERSION=3.7.1

RUN curl ... | tar -xz -C /opt/kafka   # Download & extract Kafka binary

COPY server.properties /opt/kafka/config/kraft/server.properties
COPY entrypoint.sh /entrypoint.sh

EXPOSE 9092
ENTRYPOINT ["/entrypoint.sh"]
```

```bash
# kafka/entrypoint.sh — smart init script
if [ ! -f "${KAFKA_LOG_DIRS}/meta.properties" ]; then
    # First start: generate cluster UUID and format storage
    CLUSTER_ID=$(kafka-storage.sh random-uuid)
    kafka-storage.sh format -t "$CLUSTER_ID" -c "$PROPS"
fi
# Start broker (subsequent restarts skip the format step)
exec kafka-server-start.sh "$PROPS"
```

---

## 3. Debug Kafka in Container

### 3.1 Container Health Check

```powershell
# Check if Kafka container is running
docker-compose ps

# View live broker logs
docker-compose logs -f kafka

# View last 100 lines
docker-compose logs --tail=100 kafka
```

Expected healthy output:
```
kafka-1 | [KafkaServer] started (kafka.server.KafkaServer)
```

---

### 3.2 Enter the Container Shell

```powershell
# Open a shell inside the Kafka container
docker-compose exec kafka bash

# Now you're inside — all kafka-*.sh scripts are on PATH
```

---

### 3.3 Topic Management

```bash
# ── List all topics ─────────────────────────────────────────────
kafka-topics.sh --bootstrap-server localhost:9092 --list

# ── Describe a topic (partitions, replicas, leaders) ────────────
kafka-topics.sh --bootstrap-server localhost:9092 \
    --describe --topic my-topic

# Expected output:
# Topic: my-topic  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
# Topic: my-topic  Partition: 1  Leader: 1  Replicas: 1  Isr: 1
# Topic: my-topic  Partition: 2  Leader: 1  Replicas: 1  Isr: 1

# ── Create a topic manually ──────────────────────────────────────
kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic test-topic --partitions 3 --replication-factor 1

# ── Delete a topic ───────────────────────────────────────────────
kafka-topics.sh --bootstrap-server localhost:9092 \
    --delete --topic test-topic
```

---

### 3.4 Read Messages Directly from Broker

```bash
# ── Read ALL messages from the beginning ────────────────────────
kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic my-topic \
    --from-beginning

# ── Read only NEW messages (live tail) ──────────────────────────
kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic my-topic

# ── Read with metadata (partition + offset + timestamp) ─────────
kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic my-topic \
    --from-beginning \
    --property print.partition=true \
    --property print.offset=true \
    --property print.timestamp=true

# ── Read from a specific partition only ─────────────────────────
kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic my-topic \
    --partition 0 \
    --offset earliest
```

---

### 3.5 Publish Messages Directly from Broker

```bash
# ── Interactive producer (type messages, press Enter to send) ───
kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic my-topic

# ── Send with a key ──────────────────────────────────────────────
kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic my-topic \
    --property parse.key=true \
    --property key.separator=:
# Then type:  user-1:Hello from console
```

---

### 3.6 Inspect Consumer Groups

```bash
# ── List all consumer groups ─────────────────────────────────────
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# ── Describe a group (offsets + lag) ────────────────────────────
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe --group demo-group

# Output columns:
# GROUP        TOPIC     PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
# demo-group   my-topic  0          5               5               0   ← no lag ✅
# demo-group   my-topic  1          2               2               0
# demo-group   my-topic  2          1               1               0

# ── Reset offsets (re-consume from beginning) ────────────────────
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --group demo-group \
    --topic my-topic \
    --reset-offsets --to-earliest --execute
```

**LAG** = `LOG-END-OFFSET - CURRENT-OFFSET`. A growing lag means your consumer is falling behind.

---

### 3.7 Check Broker Metadata

```bash
# Describe the cluster and all brokers
kafka-metadata-quorum.sh \
    --bootstrap-server localhost:9092 describe --status

# List broker configs
kafka-configs.sh --bootstrap-server localhost:9092 \
    --describe --entity-type brokers --entity-name 1
```

---

### 3.8 Common Problems & Fixes

| Symptom | Cause | Fix |
|---|---|---|
| `Connection refused localhost:9092` | Container not running | `docker-compose up -d` |
| `LEADER_NOT_AVAILABLE` on startup | Topic being created, brief delay | Wait 2–3s, retry |
| Consumer not receiving messages | Wrong `group-id` or `auto-offset-reset: latest` | Use `earliest` or reset offsets |
| `Port 9092 already in use` | Another Kafka/process on 9092 | `docker-compose down` then `up -d` |
| Messages lost on restart | No volume mounted | Check `volumes:` in `docker-compose.yaml` |
| `meta.properties` missing error | Corrupt storage | `docker-compose down -v` (⚠ deletes data) then `up -d` |

---

## 4. Testing Guide

### 4.1 Start Everything

```powershell
# Terminal 1 — Start Kafka broker
cd D:\DevOps\kafka-practical
docker-compose up -d

# Verify broker is healthy
docker-compose logs kafka | Select-String "started"

# Terminal 2 — Start Spring Boot app
.\mvnw.cmd spring-boot:run
# Wait for: "Started KafkaApplication in X seconds"
```

---

### 4.2 Test 1 — Simple Message (No Key)

```powershell
# Windows PowerShell
Invoke-RestMethod -Method POST `
    -Uri "http://localhost:8081/api/kafka/send?message=HelloKafka"

# Git Bash / WSL / curl.exe
curl -X POST "http://localhost:8081/api/kafka/send?message=HelloKafka"
```

**Expected HTTP response:**
```
Message sent: HelloKafka
```

**Expected app logs:**
```
[Producer]  Sent [HelloKafka] -> partition=0, offset=0
[Consumer]  Received message: HelloKafka
[Consumer]  Received record -> topic=my-topic, partition=0, offset=0, key=null, value=HelloKafka
```

---

### 4.3 Test 2 — Keyed Message (Deterministic Partition)

```powershell
# PowerShell
Invoke-RestMethod -Method POST `
    -Uri "http://localhost:8081/api/kafka/send-keyed" `
    -ContentType "application/json" `
    -Body '{"key":"user-1","message":"Hello from user 1"}'

# Git Bash / WSL
curl -X POST http://localhost:8081/api/kafka/send-keyed \
    -H "Content-Type: application/json" \
    -d '{"key":"user-1","message":"Hello from user 1"}'
```

**Expected app logs:**
```
Sent key=[user-1] value=[Hello from user 1] -> partition=2, offset=0
Received record -> topic=my-topic, partition=2, offset=0, key=user-1, value=Hello from user 1
```

> **Key rule:** The same key (`user-1`) **always** goes to the same partition.  
> Send 5 more messages with `"key":"user-1"` — all land on partition 2, offsets 1 through 5.

---

### 4.4 Test 3 — Verify Partition Routing by Key

Send the same key multiple times and a different key:

```bash
# All "user-1" messages → same partition
curl -X POST http://localhost:8081/api/kafka/send-keyed \
    -H "Content-Type: application/json" \
    -d '{"key":"user-1","message":"msg A"}'

curl -X POST http://localhost:8081/api/kafka/send-keyed \
    -H "Content-Type: application/json" \
    -d '{"key":"user-1","message":"msg B"}'

# "user-2" → different partition
curl -X POST http://localhost:8081/api/kafka/send-keyed \
    -H "Content-Type: application/json" \
    -d '{"key":"user-2","message":"msg C"}'
```

**Expected logs:**
```
Sent key=[user-1] value=[msg A] -> partition=2, offset=0
Sent key=[user-1] value=[msg B] -> partition=2, offset=1   ← same partition, next offset
Sent key=[user-2] value=[msg C] -> partition=0, offset=1   ← different partition
```

---

### 4.5 Test 4 — Verify from Inside the Container

```powershell
# Open container shell
docker-compose exec kafka bash

# Read all messages from the beginning
kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic my-topic \
    --from-beginning \
    --property print.partition=true \
    --property print.offset=true
```

**Expected output:**
```
Partition:0	Offset:0	HelloKafka
Partition:2	Offset:0	Hello from user 1
Partition:2	Offset:1	msg A
Partition:2	Offset:2	msg B
Partition:0	Offset:1	msg C
```

---

### 4.6 Test 5 — Consumer Lag Check

Send 10 messages then immediately check lag:

```bash
# Inside container — check consumer group lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe --group demo-group
```

**Expected output (no lag = healthy):**
```
GROUP        TOPIC     PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
demo-group   my-topic  0          4               4               0
demo-group   my-topic  1          2               2               0
demo-group   my-topic  2          3               3               0
```

---

### 4.7 Test 6 — Replay Messages (Reset Offsets)

Re-consume all messages from the beginning without restarting:

```bash
# 1. Stop the Spring Boot app first (Ctrl+C)

# 2. Inside container — reset offsets to earliest
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --group demo-group \
    --topic my-topic \
    --reset-offsets --to-earliest --execute

# 3. Restart the app — all messages are consumed again
.\mvnw.cmd spring-boot:run
```

---

### 4.8 Quick Test Cheatsheet

```powershell
# ── Send simple message ──────────────────────────────────────────
curl -X POST "http://localhost:8081/api/kafka/send?message=Hello"

# ── Send keyed message ───────────────────────────────────────────
curl -X POST http://localhost:8081/api/kafka/send-keyed \
     -H "Content-Type: application/json" \
     -d '{"key":"user-1","message":"Hi"}'

# ── List topics (inside container) ──────────────────────────────
docker-compose exec kafka kafka-topics.sh \
    --bootstrap-server localhost:9092 --list

# ── Read all messages (inside container) ────────────────────────
docker-compose exec kafka kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic my-topic --from-beginning

# ── Check consumer lag (inside container) ───────────────────────
docker-compose exec kafka kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --describe --group demo-group

# ── Reset offsets to replay all messages ────────────────────────
docker-compose exec kafka kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --group demo-group --topic my-topic \
    --reset-offsets --to-earliest --execute
```

---

## 5. Production Best Practices

### 5.1 Broker: Cluster & Replication

A single-broker setup loses all data if the node goes down. Production requires **at least 3 brokers** so a replica can take over without data loss.

```
Dev (current)             Production (minimum)
──────────────            ────────────────────────────────────────
1 broker                  3 brokers across 3 availability zones
1 replica                 3 replicas (RF=3)
no fault tolerance        survives 1 broker failure without data loss
```

**`server.properties` changes for production:**

```properties
# Replication — never run RF=1 in production
default.replication.factor=3
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2       # At least 2 in-sync replicas before ACK

# Durability — flush to disk more aggressively
log.flush.interval.messages=10000     # Flush every 10k messages
log.flush.interval.ms=1000            # Or every 1 second, whichever comes first

# Retention — tune to your storage budget
log.retention.hours=72                # 3 days (adjust per use case)
log.retention.bytes=10737418240       # 10 GB cap per partition
```

---

### 5.2 Producer: Durability & Reliability

The default producer settings can **silently lose messages** under broker failures. Harden the producer in `application.yml`:

```yaml
spring:
  kafka:
    producer:
      # ── Durability ─────────────────────────────────────────────
      acks: all              # Wait for ALL in-sync replicas to acknowledge
                             # (not just the leader). Never use acks=0 or acks=1.

      # ── Retries ────────────────────────────────────────────────
      retries: 10            # Retry transient network/broker failures
      properties:
        retry.backoff.ms: 300
        delivery.timeout.ms: 120000    # Total time budget for one send (2 min)
        request.timeout.ms: 30000      # Timeout per broker request

      # ── Idempotence — exactly-once delivery ────────────────────
      properties:
        enable.idempotence: true       # Prevents duplicates on retry
                                       # Requires: acks=all, retries>0

      # ── Batching & Throughput ──────────────────────────────────
      batch-size: 65536                # Batch up to 64 KB before sending
      buffer-memory: 33554432          # 32 MB internal send buffer
      properties:
        linger.ms: 10                  # Wait up to 10ms to fill a batch
        compression.type: lz4          # lz4 = best balance speed/ratio
                                       # Options: none, gzip, snappy, lz4, zstd
```

> **Rule of thumb:**  
> `acks=all` + `enable.idempotence=true` + `retries>0` = **at-least-once** with deduplication  
> Add transactions (`transactional.id`) for true **exactly-once** semantics.

---

### 5.3 Consumer: Reliability & Throughput

```yaml
spring:
  kafka:
    consumer:
      # ── Offset Management ──────────────────────────────────────
      enable-auto-commit: false        # NEVER use auto-commit in production
                                       # It commits before processing completes
                                       # → messages can be lost on crash

      # ── Polling ────────────────────────────────────────────────
      max-poll-records: 100            # Process 100 records per poll()
      properties:
        max.poll.interval.ms: 300000   # Max time between polls before
                                       # Kafka considers consumer dead (5 min)
        session.timeout.ms: 30000      # Heartbeat timeout (30s)
        heartbeat.interval.ms: 10000   # Send heartbeat every 10s

      # ── Offset reset — production should be "latest" ───────────
      auto-offset-reset: latest        # Only process NEW messages on first start
                                       # Use "earliest" only for event replay
    listener:
      ack-mode: MANUAL_IMMEDIATE       # Commit offset only AFTER successful processing
      concurrency: 3                   # 3 consumer threads = 1 per partition
```

**Manual ACK pattern in your consumer:**

```java
@KafkaListener(topics = "${app.kafka.topic}", groupId = "demo-group")
public void listen(String message, Acknowledgment ack) {
    try {
        // process message...
        log.info("Processed: {}", message);
        ack.acknowledge();             // ✅ commit only on success
    } catch (Exception e) {
        log.error("Processing failed: {}", e.getMessage());
        // Do NOT ack → message will be redelivered
    }
}
```

---

### 5.4 Topic Design

```bash
# Production topic — more partitions = more parallelism
kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic orders \
    --partitions 12 \             # Rule: num_partitions >= max_consumers_in_group
    --replication-factor 3 \      # Always 3 in production
    --config retention.ms=604800000 \   # 7 days in ms
    --config min.insync.replicas=2 \    # Refuse writes if < 2 replicas are in sync
    --config compression.type=lz4
```

**Partition count guidelines:**

| Throughput Target | Recommended Partitions | Notes |
|---|---|---|
| < 10 MB/s | 6 | Enough for most microservices |
| 10–100 MB/s | 12–24 | Match your consumer count |
| > 100 MB/s | 50+ | Profile first; more partitions = more overhead |

> ⚠️ **You cannot reduce partitions** after creation — only increase. Start conservative and scale up.

---

### 5.5 Security

**Never run Kafka without auth in production.** The current setup uses `PLAINTEXT` (no encryption, no auth).

#### Enable TLS + SASL/SCRAM in `server.properties`:

```properties
# Replace PLAINTEXT listener with SSL + SASL
listeners=SASL_SSL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
advertised.listeners=SASL_SSL://your-broker-host:9092
listener.security.protocol.map=SASL_SSL:SASL_SSL,CONTROLLER:PLAINTEXT

# TLS
ssl.keystore.location=/certs/kafka.keystore.jks
ssl.keystore.password=changeit
ssl.truststore.location=/certs/kafka.truststore.jks
ssl.truststore.password=changeit
ssl.client.auth=required

# SASL
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512
sasl.enabled.mechanisms=SCRAM-SHA-512
```

#### Spring Boot client with SASL/SSL:

```yaml
spring:
  kafka:
    security:
      protocol: SASL_SSL
    ssl:
      trust-store-location: classpath:kafka.truststore.jks
      trust-store-password: changeit
    properties:
      sasl.mechanism: SCRAM-SHA-512
      sasl.jaas.config: >
        org.apache.kafka.common.security.scram.ScramLoginModule required
        username="app-user"
        password="${KAFKA_PASSWORD}";
```

---

### 5.6 Monitoring & Alerting

#### Key metrics to watch (via JMX / Prometheus):

| Metric | Alert Threshold | What It Means |
|---|---|---|
| `kafka.consumer.lag` | > 10,000 | Consumer falling behind — scale up |
| `UnderReplicatedPartitions` | > 0 | A broker is down or replication is broken |
| `ActiveControllerCount` | != 1 | Split-brain or no active controller |
| `RequestHandlerAvgIdlePercent` | < 0.3 (30%) | Broker CPU saturated |
| `BytesInPerSec` / `BytesOutPerSec` | Near NIC limit | Network bottleneck |
| `ProducerRequestRate` (errors) | > 0 | Producers are failing |

#### Enable Prometheus metrics in Spring Boot (`pom.xml`):

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-actuator</artifactId>
</dependency>
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>
```

```yaml
# application.yml
management:
  endpoints:
    web:
      exposure:
        include: health, metrics, prometheus
  metrics:
    tags:
      application: ${spring.application.name}
```

Then scrape `http://localhost:8081/actuator/prometheus` with Prometheus.

---

### 5.7 Dead Letter Queue (DLQ) Pattern

When a consumer fails to process a message after all retries, send it to a DLQ for inspection instead of dropping it.

```yaml
spring:
  kafka:
    listener:
      # Retry 3 times, then send to DLQ
      retry-topic-suffix: -retry
      dlt-topic-suffix: -dlt         # Dead Letter Topic
```

```java
@RetryableTopic(
    attempts = "3",
    backoff = @Backoff(delay = 1000, multiplier = 2.0),
    dltStrategy = DltStrategy.FAIL_ON_ERROR
)
@KafkaListener(topics = "${app.kafka.topic}", groupId = "demo-group")
public void listen(String message) {
    // If this throws 3 times → message goes to my-topic-dlt
    processMessage(message);
}

@DltHandler
public void handleDlt(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
    log.error("DLQ received failed message from topic={}: {}", topic, message);
    // Alert, store in DB, or send to Slack
}
```

---

### 5.8 Dev vs. Production Config Comparison

| Setting | This Project (Dev) | Production |
|---|---|---|
| Brokers | 1 | 3+ (across AZs) |
| Replication factor | 1 | 3 |
| `acks` | default (1) | `all` |
| `enable.idempotence` | false | true |
| `enable-auto-commit` | true | false |
| `ack-mode` | AUTO | `MANUAL_IMMEDIATE` |
| Security | `PLAINTEXT` | `SASL_SSL` |
| Partitions | 3 | 12+ (match consumer count) |
| Monitoring | none | Prometheus + Grafana + alerts |
| Retention | 168h (7 days) | Per business SLA |
| Dead Letter Queue | none | Required |
| Schema registry | none | Confluent / AWS Glue |

---

### 5.9 Production Docker Compose Template

```yaml
# docker-compose.prod.yml — 3-broker KRaft cluster
version: '3.8'
services:
  kafka-1:
    image: apache/kafka:3.7.1
    hostname: kafka-1
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-1:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"  # Prevent accidental topic creation
    volumes:
      - kafka-1-data:/var/lib/kafka/data
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: "1.5"

  kafka-2:
    image: apache/kafka:3.7.1
    hostname: kafka-2
    environment:
      KAFKA_NODE_ID: 2
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-2:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
    volumes:
      - kafka-2-data:/var/lib/kafka/data

  kafka-3:
    image: apache/kafka:3.7.1
    hostname: kafka-3
    environment:
      KAFKA_NODE_ID: 3
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-3:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
    volumes:
      - kafka-3-data:/var/lib/kafka/data

volumes:
  kafka-1-data:
  kafka-2-data:
  kafka-3-data:
```

---

## Project Structure Reference

```
kafka-practical/
├── docker-compose.yaml              # Starts Kafka on port 9092
├── kafka/
│   ├── Dockerfile                   # Kafka 3.7.1 on JRE 17 Alpine
│   ├── server.properties            # KRaft broker config
│   └── entrypoint.sh                # Auto-formats storage on first start
└── src/main/java/com/example/kafka/
    ├── KafkaApplication.java        # Spring Boot entry point
    ├── config/
    │   └── KafkaConfig.java         # Creates my-topic (3 partitions)
    ├── producer/
    │   └── MessageProducer.java     # send() and sendWithKey()
    ├── consumer/
    │   └── MessageConsumer.java     # Two @KafkaListener methods
    └── controller/
        └── MessageController.java   # POST /api/kafka/send + /send-keyed
```
