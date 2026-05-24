# Kafka Documentation
> Spring Boot 3.3 + **Confluent Platform 7.7** · Avro + Schema Registry · KRaft Mode · Docker

---

## Table of Contents
1. [How Kafka Works](#1-how-kafka-works)
2. [Kafka Configuration](#2-kafka-configuration)
3. [Debug Kafka in Container](#3-debug-kafka-in-container)
4. [Testing Guide](#4-testing-guide)
5. [Production Best Practices](#5-production-best-practices)

---

## 1. How Kafka Works

### 1.1 Confluent Platform Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        CONFLUENT PLATFORM STACK                              │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                      Control Center  :9021                          │    │
│  │            (Web UI — topics, consumers, schemas, lag)               │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│            │                    │                    │                       │
│  ┌─────────▼──────┐   ┌────────▼────────┐  ┌───────▼────────┐             │
│  │  Schema        │   │   Kafka Broker  │  │  REST Proxy    │             │
│  │  Registry      │   │   (KRaft)       │  │  :8082         │             │
│  │  :8081         │   │   :9092         │  │  (HTTP API)    │             │
│  └─────────┬──────┘   └────────┬────────┘  └───────┬────────┘             │
│            │   register/fetch  │  produce/consume   │                       │
│            │   schema          │                    │                       │
│  ┌─────────▼──────────────────▼────────────────────▼────────────────┐      │
│  │                    Spring Boot App  :8085                         │      │
│  │                                                                   │      │
│  │  POST /api/kafka/send          → MessageProducer (String)        │      │
│  │  POST /api/kafka/send-keyed    → MessageProducer (String+key)    │      │
│  │  POST /api/kafka/avro/send     → AvroMessageProducer             │      │
│  │  POST /api/kafka/avro/send-keyed → AvroMessageProducer+key       │      │
│  └───────────────────────────────────────────────────────────────────┘      │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

### 1.2 Key Components

| Component | Description | In This Project |
|---|---|---|
| **Broker** | Stores and serves messages | `confluentinc/cp-kafka:7.7.0` on port `9092` |
| **Schema Registry** | Validates and versions Avro/JSON/Protobuf schemas | `cp-schema-registry:7.7.0` on port `8081` |
| **REST Proxy** | Produce/consume via plain HTTP — no Kafka client needed | `cp-kafka-rest:7.7.0` on port `8082` |
| **Control Center** | Enterprise web UI — topics, consumers, schemas, lag | `cp-enterprise-control-center:7.7.0` on port `9021` |
| **String Producer** | Publishes plain text messages | `MessageProducer.java` |
| **Avro Producer** | Publishes schema-validated Avro messages | `AvroMessageProducer.java` |
| **String Consumer** | Reads plain text messages | `MessageConsumer.java` |
| **Avro Consumer** | Reads and deserializes Avro messages | `AvroMessageConsumer.java` |
| **Topic** | Named channel for messages | `my-topic`, `my-topic-avro` |
| **Partition** | Ordered, immutable log within a topic | 3 partitions per topic |
| **Consumer Group** | Group of consumers sharing partition load | `demo-group`, `demo-group-avro` |

---

### 1.3 String Message Flow

```
Step 1: HTTP POST to Spring Boot
─────────────────────────────────
  curl -X POST "http://localhost:8085/api/kafka/send?message=Hello"
                      │
                      ▼
Step 2: Controller → Producer
──────────────────────────────
  MessageController.send()
      └── MessageProducer.send("Hello")
              └── kafkaTemplate.send("my-topic", "Hello")

Step 3: Broker assigns partition
─────────────────────────────────
  No key → round-robin:   "Hello" → partition=0, offset=0
  With key → hash(key):   "Hello" → always same partition

Step 4: Broker persists to disk
─────────────────────────────────
  Written to Docker volume: broker-data:/tmp/kraft-combined-logs
  Retained for 7 days (default)

Step 5: Consumers receive
──────────────────────────
  demo-group          → MessageConsumer.listen()             → logs value
  demo-group-advanced → MessageConsumer.listenWithMetadata() → logs full record
```

---

### 1.4 Avro Message Flow (Schema Registry)

```
Step 1: HTTP POST
──────────────────
  curl -X POST "http://localhost:8085/api/kafka/avro/send?message=Hello"
                          │
                          ▼
Step 2: AvroMessageProducer builds a KafkaMessage object
──────────────────────────────────────────────────────────
  KafkaMessage {
    id:        "550e8400-e29b-41d4-a716-446655440000"
    key:       null
    content:   "Hello"
    timestamp: 1779638788106
  }

Step 3: KafkaAvroSerializer checks Schema Registry
────────────────────────────────────────────────────
  First send: POST http://schema-registry:8081/subjects/my-topic-avro-value/versions
              ← schema registered, assigned schema ID = 1

  Subsequent sends: schema ID already cached locally, no HTTP call

Step 4: Message wire format (Confluent binary encoding)
─────────────────────────────────────────────────────────
  [ 0x00 | schema-id (4 bytes) | avro-payload ]
   magic     e.g. 0x00000001    binary-encoded fields

Step 5: Broker stores encoded bytes
────────────────────────────────────
  Stored in my-topic-avro partitions

Step 6: AvroMessageConsumer deserializes
──────────────────────────────────────────
  KafkaAvroDeserializer reads schema ID from bytes
  → fetches schema from Schema Registry (cached after first fetch)
  → deserializes bytes back to KafkaMessage object
  → AvroMessageConsumer.listen() logs all fields
```

---

### 1.5 Schema Registry — Why It Matters

```
Without Schema Registry          With Schema Registry
─────────────────────────        ─────────────────────────────────────
Producer sends raw JSON          Producer registers schema on first send
Consumer hopes schema matches    Consumer fetches schema by ID — always compatible
Schema drift → silent failures   Breaking changes rejected at publish time
No versioning                    Full schema version history + compatibility rules
```

**Compatibility modes** (set per subject):

| Mode | Rule |
|---|---|
| `BACKWARD` | New schema can read old data — safe for consumer upgrades first |
| `FORWARD` | Old schema can read new data — safe for producer upgrades first |
| `FULL` | Both directions — safest, most restrictive |
| `NONE` | No checks — dev only |

---

### 1.6 Partitions & Consumer Groups

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

### 1.7 KRaft Mode (No Zookeeper)

This project uses **KRaft** — Kafka's built-in Raft consensus that eliminates Zookeeper. Confluent Platform 7.4+ fully supports KRaft in production.

```
Old:    Broker ←── Zookeeper (separate process, separate ops burden)
KRaft:  Broker + Controller combined (single process, no Zookeeper!)
```

Configured via environment variables in `docker-compose.yaml`:
```yaml
KAFKA_PROCESS_ROLES: broker,controller     # dual role
KAFKA_NODE_ID: 1
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@broker:29093
CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk        # fixed UUID → stable across restarts
```

---

## 2. Kafka Configuration

### 2.1 Port Map

| Port | Service | Notes |
|---|---|---|
| `9092` | Kafka Broker (external) | Used by Spring Boot app and CLI tools |
| `29092` | Kafka Broker (internal) | Used by Schema Registry and REST Proxy inside Docker network |
| `29093` | KRaft Controller | Internal Raft consensus traffic only |
| `9101` | JMX | Broker metrics for Prometheus/Grafana |
| `8081` | Schema Registry | REST API for schema CRUD |
| `8082` | REST Proxy | HTTP produce/consume API |
| `9021` | Control Center | Web UI |
| `8085` | Spring Boot App | Moved from 8081 to avoid conflict with Schema Registry |

---

### 2.2 Confluent Platform Stack — `docker-compose.yaml`

```yaml
services:

  broker:                                        # Kafka broker in KRaft mode
    image: confluentinc/cp-kafka:7.7.0
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller     # Combined broker + controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@broker:29093
      # Three listeners:
      #   PLAINTEXT      → broker-to-broker (internal Docker network)
      #   PLAINTEXT_HOST → client-facing (host machine access)
      #   CONTROLLER     → Raft consensus traffic
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_LISTENERS: PLAINTEXT://broker:29092,PLAINTEXT_HOST://0.0.0.0:9092,CONTROLLER://broker:29093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk        # Fixed UUID — prevents re-format on restart
    ports:
      - "9092:9092"    # Expose only the host-facing listener
      - "9101:9101"    # JMX metrics
    volumes:
      - broker-data:/tmp/kraft-combined-logs     # Persist messages across restarts
    healthcheck:
      test: ["CMD", "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"]
      interval: 30s
      timeout: 10s
      retries: 5

  schema-registry:
    image: confluentinc/cp-schema-registry:7.7.0
    depends_on:
      broker:
        condition: service_healthy              # Wait for broker healthcheck to pass
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: broker:29092   # Uses internal listener
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081

  rest-proxy:
    image: confluentinc/cp-kafka-rest:7.7.0
    depends_on:
      broker:
        condition: service_healthy
      schema-registry:
        condition: service_healthy
    ports:
      - "8082:8082"
    environment:
      KAFKA_REST_BOOTSTRAP_SERVERS: broker:29092
      KAFKA_REST_SCHEMA_REGISTRY_URL: http://schema-registry:8081

  control-center:
    image: confluentinc/cp-enterprise-control-center:7.7.0
    depends_on:
      broker:
        condition: service_healthy
      schema-registry:
        condition: service_healthy
    ports:
      - "9021:9021"
    environment:
      CONTROL_CENTER_BOOTSTRAP_SERVERS: broker:29092
      CONTROL_CENTER_SCHEMA_REGISTRY_URL: http://schema-registry:8081
      CONTROL_CENTER_REPLICATION_FACTOR: 1
```

> ⚠️ **Control Center licensing**: Free for 30 days. After that, monitoring features require a Confluent license. For unlimited free monitoring use Kafka UI (`provectuslabs/kafka-ui`) instead.

---

### 2.3 Spring Boot Configuration — `application.yml`

```yaml
server:
  port: 8085   # 8081 = Schema Registry, 8082 = REST Proxy

spring:
  kafka:
    bootstrap-servers: localhost:9092

    # ── String producer (plain text) ───────────────────────────────
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer

    # ── String consumer ────────────────────────────────────────────
    consumer:
      group-id: demo-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer

    # ── Shared Confluent properties (used by Avro beans in KafkaConfig) ─
    properties:
      schema.registry.url: http://localhost:8081

# ── Actuator / Prometheus ───────────────────────────────────────────
management:
  endpoints:
    web:
      exposure:
        include: health, info, metrics, prometheus

app:
  kafka:
    topic: my-topic           # String topic
    avro-topic: my-topic-avro # Avro topic
```

---

### 2.4 Avro Schema — `src/main/avro/KafkaMessage.avsc`

```json
{
  "type": "record",
  "name": "KafkaMessage",
  "namespace": "com.example.kafka.avro",
  "fields": [
    { "name": "id",        "type": "string" },
    { "name": "key",       "type": ["null", "string"], "default": null },
    { "name": "content",   "type": "string" },
    { "name": "timestamp", "type": { "type": "long", "logicalType": "timestamp-millis" } }
  ]
}
```

The `avro-maven-plugin` generates `KafkaMessage.java` automatically during `mvn compile` into `src/main/java/com/example/kafka/avro/`. The generated class is excluded from git via `.gitignore`.

---

### 2.5 Avro Kafka Beans — `KafkaConfig.java`

```java
// Avro ProducerFactory — uses KafkaAvroSerializer wired to Schema Registry
@Bean
public ProducerFactory<String, Object> avroProducerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    props.put("schema.registry.url", schemaRegistryUrl);
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);   // no duplicates on retry
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    return new DefaultKafkaProducerFactory<>(props);
}

// Avro ConsumerFactory — fetches schema by ID from Schema Registry
@Bean
public ConsumerFactory<String, Object> avroConsumerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    props.put("schema.registry.url", schemaRegistryUrl);
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true); // → KafkaMessage.class
    return new DefaultKafkaConsumerFactory<>(props);
}
```

---

### 2.6 Topic Configuration — `KafkaConfig.java`

```java
@Bean public NewTopic myTopic() {
    return TopicBuilder.name("my-topic")
            .partitions(3).replicas(1).build();
}

@Bean public NewTopic myAvroTopic() {
    return TopicBuilder.name("my-topic-avro")
            .partitions(3).replicas(1).build();
}
```

#### Partition count guidelines:

| Consumers | Partitions | Result |
|---|---|---|
| 1 consumer | 3 partitions | 1 consumer handles all 3 — no parallelism |
| 3 consumers | 3 partitions | 1 partition per consumer — full parallelism ✅ |
| 4 consumers | 3 partitions | 1 consumer idles — wasted resource |

---

## 3. Debug Kafka in Container

### 3.1 Check All Services

```powershell
# Status of all Confluent Platform services
docker-compose ps

# Expected:
# broker           running (healthy)
# schema-registry  running (healthy)
# rest-proxy       running
# control-center   running
```

```powershell
# Live logs — watch all services at once
docker-compose logs -f

# Logs for a specific service
docker-compose logs -f broker
docker-compose logs -f schema-registry
docker-compose logs --tail=50 control-center
```

**Healthy broker output:**
```
broker | [KafkaServer id=1] started
```

---

### 3.2 Enter the Broker Container Shell

```powershell
# All kafka-*.sh scripts are on PATH inside the container
docker-compose exec broker bash
```

> **Note:** The container is now named `broker` (not `kafka`).  
> Use `docker-compose exec broker <cmd>` for all CLI operations.

---

### 3.3 Topic Management

```bash
# ── List all topics ─────────────────────────────────────────────
kafka-topics.sh --bootstrap-server localhost:9092 --list
# my-topic
# my-topic-avro
# _schemas                    ← Schema Registry internal topic
# _confluent-monitoring       ← Control Center internal topic

# ── Describe a topic ────────────────────────────────────────────
kafka-topics.sh --bootstrap-server localhost:9092 \
    --describe --topic my-topic

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
# ── Read ALL String messages from my-topic ───────────────────────
kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic my-topic \
    --from-beginning \
    --property print.partition=true \
    --property print.offset=true \
    --property print.timestamp=true

# ── Read Avro messages (decoded via Schema Registry) ─────────────
kafka-avro-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic my-topic-avro \
    --from-beginning \
    --property schema.registry.url=http://schema-registry:8081 \
    --property print.partition=true \
    --property print.offset=true
# Output: {"id":"550e...","key":null,"content":"Hello","timestamp":1779638788106}

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
# ── String messages ──────────────────────────────────────────────
kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic my-topic
# (type message, press Enter)

# ── String messages with key ─────────────────────────────────────
kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic my-topic \
    --property parse.key=true \
    --property key.separator=:
# Type: user-1:Hello from console

# ── Avro messages (schema must already be registered) ────────────
kafka-avro-console-producer \
    --bootstrap-server localhost:9092 \
    --topic my-topic-avro \
    --property schema.registry.url=http://schema-registry:8081 \
    --property value.schema='{"type":"record","name":"KafkaMessage","namespace":"com.example.kafka.avro","fields":[{"name":"id","type":"string"},{"name":"key","type":["null","string"],"default":null},{"name":"content","type":"string"},{"name":"timestamp","type":{"type":"long","logicalType":"timestamp-millis"}}]}'
# Type: {"id":"1","key":null,"content":"test","timestamp":1700000000000}
```

---

### 3.6 Inspect Consumer Groups

```bash
# ── List all consumer groups ─────────────────────────────────────
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
# demo-group
# demo-group-advanced
# demo-group-avro

# ── Describe a group (offsets + lag) ────────────────────────────
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe --group demo-group

# GROUP        TOPIC     PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
# demo-group   my-topic  0          5               5               0  ✅
# demo-group   my-topic  1          2               2               0
# demo-group   my-topic  2          3               3               0

# ── Reset offsets (re-consume from beginning) ────────────────────
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --group demo-group \
    --topic my-topic \
    --reset-offsets --to-earliest --execute
```

**LAG** = `LOG-END-OFFSET − CURRENT-OFFSET`. A growing lag means your consumer is falling behind.

---

### 3.7 Debug Schema Registry

```bash
# ── From host machine (port 8081 exposed) ───────────────────────

# List all registered subjects
curl http://localhost:8081/subjects
# ["my-topic-avro-value"]

# Get all versions of a subject
curl http://localhost:8081/subjects/my-topic-avro-value/versions
# [1]

# Get schema by version
curl http://localhost:8081/subjects/my-topic-avro-value/versions/1
# {"subject":"my-topic-avro-value","version":1,"id":1,"schema":"{...}"}

# Get schema by ID
curl http://localhost:8081/schemas/ids/1

# Check compatibility before evolving a schema
curl -X POST http://localhost:8081/compatibility/subjects/my-topic-avro-value/versions/latest \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d '{"schema": "{\"type\":\"record\",\"name\":\"KafkaMessage\",\"namespace\":\"com.example.kafka.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"key\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"content\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},{\"name\":\"source\",\"type\":[\"null\",\"string\"],\"default\":null}]}"}'
# {"is_compatible":true}

# Delete a subject (dev only — irreversible in production)
curl -X DELETE http://localhost:8081/subjects/my-topic-avro-value
```

---

### 3.8 Common Problems & Fixes

| Symptom | Cause | Fix |
|---|---|---|
| `Connection refused localhost:9092` | Broker not running | `docker-compose up -d` |
| `LEADER_NOT_AVAILABLE` on startup | Topic being created, brief delay | Wait 2–3s, retry |
| `Schema not found` on consumer start | Schema Registry not ready | Wait for `service_healthy` check; check `docker-compose ps` |
| `Port 8081 conflict` | Schema Registry vs Spring Boot both on 8081 | Spring Boot is now on port **8085** — ensure you're using correct port |
| Consumer not receiving messages | Wrong `group-id` or `auto-offset-reset` | Check group name; use `earliest`; or reset offsets |
| `Connection refused localhost:9021` | Control Center still booting | Takes ~60s — wait and retry |
| Messages lost on restart | Volume not mounted | Check `volumes: broker-data:` in `docker-compose.yaml` |
| Avro deserialization error | Schema ID mismatch | Clear broker data + Schema Registry: `docker-compose down -v` ⚠ |

---

## 4. Testing Guide

### 4.1 Start Everything

```powershell
# Terminal 1 — Start Confluent Platform (all 4 services)
cd D:\DevOps\kafka-practical
docker-compose up -d

# Watch until all services are healthy (~60-90s first run, ~30s after)
docker-compose ps

# Terminal 2 — Start Spring Boot app
.\mvnw.cmd spring-boot:run
# Wait for: "Started KafkaApplication in X seconds"
# App is available at http://localhost:8085
```

---

### 4.2 Test 1 — String Message (No Key)

```powershell
# PowerShell
Invoke-RestMethod -Method POST -Uri "http://localhost:8085/api/kafka/send?message=HelloKafka"

# Git Bash / WSL
curl -X POST "http://localhost:8085/api/kafka/send?message=HelloKafka"
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

### 4.3 Test 2 — String Keyed Message

```bash
curl -X POST http://localhost:8085/api/kafka/send-keyed \
    -H "Content-Type: application/json" \
    -d '{"key":"user-1","message":"Hello from user 1"}'
```

**Expected logs:**
```
Sent key=[user-1] value=[Hello from user 1] -> partition=2, offset=0
Received record -> topic=my-topic, partition=2, offset=0, key=user-1, value=Hello from user 1
```

> Same key always routes to the same partition — send 5 more with `"key":"user-1"` and they all land on partition 2 at increasing offsets.

---

### 4.4 Test 3 — Avro Message (Schema Registry)

```bash
# Send Avro message — schema is auto-registered on first send
curl -X POST "http://localhost:8085/api/kafka/avro/send?message=HelloAvro"
```

**Expected app logs:**
```
[Avro] Sent id=550e8400-... content='HelloAvro' -> partition=1, offset=0
[Avro] Received → topic=my-topic-avro, partition=1, offset=0 | id=550e8400-..., key=null, content='HelloAvro', sentAt=2026-05-24T16:06:28Z
```

**Verify the schema was registered:**
```powershell
# Host machine
Invoke-RestMethod http://localhost:8081/subjects
# ["my-topic-avro-value"]

Invoke-RestMethod http://localhost:8081/subjects/my-topic-avro-value/versions/1
# id=1, schema={"type":"record","name":"KafkaMessage"...}
```

---

### 4.5 Test 4 — Avro Keyed Message

```bash
curl -X POST http://localhost:8085/api/kafka/avro/send-keyed \
    -H "Content-Type: application/json" \
    -d '{"key":"user-1","message":"Avro from user 1"}'
```

**Expected logs:**
```
[Avro] Sent key='user-1' content='Avro from user 1' -> partition=2, offset=0
[Avro] Received → topic=my-topic-avro, partition=2, offset=0 | id=..., key=user-1, content='Avro from user 1'
```

---

### 4.6 Test 5 — REST Proxy (No Kafka Client Needed)

Send a message using only HTTP — no Kafka dependency in the caller:

```bash
# Produce a message via REST Proxy
curl -X POST http://localhost:8082/topics/my-topic \
    -H "Content-Type: application/vnd.kafka.json.v2+json" \
    -d '{"records":[{"value":"Hello from REST Proxy"}]}'

# Response:
# {"offsets":[{"partition":0,"offset":2,"error_code":null,"error":null}]}

# Produce with a key
curl -X POST http://localhost:8082/topics/my-topic \
    -H "Content-Type: application/vnd.kafka.json.v2+json" \
    -d '{"records":[{"key":"user-1","value":"Keyed via REST Proxy"}]}'
```

---

### 4.7 Test 6 — Control Center Web UI

1. Open **http://localhost:9021** in your browser
2. Select your cluster → **Topics** → click `my-topic`
3. Go to **Messages** tab → you can see every message with partition, offset, timestamp
4. Go to **Schema Registry** → view registered `KafkaMessage` schema and version history
5. Go to **Consumer Groups** → check lag for `demo-group` and `demo-group-avro`

---

### 4.8 Test 7 — Read Avro Messages from Container

```powershell
docker-compose exec broker bash
```
```bash
kafka-avro-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic my-topic-avro \
    --from-beginning \
    --property schema.registry.url=http://schema-registry:8081
# {"id":"550e8400-...","key":null,"content":"HelloAvro","timestamp":1779638788106}
```

---

### 4.9 Test 8 — Consumer Lag Check

```bash
# Inside broker container
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe --group demo-group-avro
```

```
GROUP           TOPIC          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
demo-group-avro my-topic-avro  0          2               2               0
demo-group-avro my-topic-avro  1          1               1               0
demo-group-avro my-topic-avro  2          1               1               0
```

---

### 4.10 Quick Test Cheatsheet

```bash
# ── String messages ──────────────────────────────────────────────
curl -X POST "http://localhost:8085/api/kafka/send?message=Hello"
curl -X POST http://localhost:8085/api/kafka/send-keyed \
     -H "Content-Type: application/json" -d '{"key":"u1","message":"Hi"}'

# ── Avro messages ────────────────────────────────────────────────
curl -X POST "http://localhost:8085/api/kafka/avro/send?message=HelloAvro"
curl -X POST http://localhost:8085/api/kafka/avro/send-keyed \
     -H "Content-Type: application/json" -d '{"key":"u1","message":"Hi Avro"}'

# ── REST Proxy ───────────────────────────────────────────────────
curl -X POST http://localhost:8082/topics/my-topic \
     -H "Content-Type: application/vnd.kafka.json.v2+json" \
     -d '{"records":[{"value":"Via REST Proxy"}]}'

# ── Schema Registry ──────────────────────────────────────────────
curl http://localhost:8081/subjects
curl http://localhost:8081/subjects/my-topic-avro-value/versions/1

# ── Broker CLI (inside container) ───────────────────────────────
docker-compose exec broker kafka-topics.sh --bootstrap-server localhost:9092 --list
docker-compose exec broker kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --group demo-group

# ── Control Center UI ────────────────────────────────────────────
# http://localhost:9021
```

---

## 5. Production Best Practices

### 5.1 Broker: Cluster & Replication

A single-broker setup loses all data if the node goes down. Production requires **at least 3 brokers** so a replica can take over without data loss.

```
Dev (current)              Production (minimum)
───────────────────         ─────────────────────────────────────────
1 broker                   3 brokers across 3 availability zones
RF=1 (no replicas)         RF=3 (survives 1 broker failure)
no Schema Registry HA      Schema Registry with 3+ instances
Control Center (trial)     Control Center with Confluent license
```

**Production broker env vars (Confluent Platform):**

```yaml
KAFKA_DEFAULT_REPLICATION_FACTOR: 3
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
KAFKA_MIN_INSYNC_REPLICAS: 2           # Refuse writes if < 2 replicas are in sync
KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"  # All topics must be explicitly created
KAFKA_LOG_RETENTION_HOURS: 72
KAFKA_LOG_RETENTION_BYTES: 10737418240    # 10 GB per partition cap
```

---

### 5.2 Schema Registry: Compatibility & Evolution

```bash
# Set compatibility mode for a subject (do this before first schema registration)
curl -X PUT http://localhost:8081/config/my-topic-avro-value \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d '{"compatibility": "BACKWARD"}'

# Check current compatibility setting
curl http://localhost:8081/config/my-topic-avro-value
```

**Safe schema evolution rules (BACKWARD compatible):**
```json
// ✅ SAFE — add optional field with default
{"name": "source", "type": ["null", "string"], "default": null}

// ✅ SAFE — remove a field (old consumers ignore unknown fields)

// ❌ BREAKING — rename a field (treated as delete + add)
// ❌ BREAKING — change field type (e.g. string → int)
// ❌ BREAKING — add required field with no default
```

**Production Schema Registry HA setup:**
```yaml
schema-registry-1:
  image: confluentinc/cp-schema-registry:7.7.0
  environment:
    SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: broker-1:29092,broker-2:29092,broker-3:29092
    SCHEMA_REGISTRY_HOST_NAME: schema-registry-1
    SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
    SCHEMA_REGISTRY_MASTER_ELIGIBILITY: "true"    # This instance can be elected leader
```

---

### 5.3 Producer: Durability & Reliability

```yaml
spring:
  kafka:
    producer:
      acks: all                          # Wait for ALL in-sync replicas
      retries: 10
      properties:
        enable.idempotence: true         # No duplicates on retry (already set in KafkaConfig)
        delivery.timeout.ms: 120000
        retry.backoff.ms: 300
        linger.ms: 10                    # Batch for 10ms to improve throughput
        compression.type: lz4
        batch-size: 65536
```

> **Avro producer already has `acks=all` and `enable.idempotence=true`** configured in `KafkaConfig.java`.  
> Apply the same settings to the String producer in `application.yml` for production.

---

### 5.4 Consumer: Reliability & Throughput

```yaml
spring:
  kafka:
    consumer:
      enable-auto-commit: false          # Never auto-commit in production
      max-poll-records: 100
      auto-offset-reset: latest          # In prod: only new messages; use earliest for replay
      properties:
        max.poll.interval.ms: 300000
        session.timeout.ms: 30000
        heartbeat.interval.ms: 10000
    listener:
      ack-mode: MANUAL_IMMEDIATE         # Commit offset only after successful processing
      concurrency: 3                     # 1 thread per partition
```

**Manual ACK pattern:**
```java
@KafkaListener(topics = "${app.kafka.topic}", groupId = "demo-group")
public void listen(String message, Acknowledgment ack) {
    try {
        processMessage(message);
        ack.acknowledge();               // ✅ commit only on success
    } catch (Exception e) {
        log.error("Failed: {}", e.getMessage());
        // No ack → message redelivered
    }
}
```

---

### 5.5 Topic Design

```bash
kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic orders \
    --partitions 12 \
    --replication-factor 3 \
    --config retention.ms=604800000 \       # 7 days
    --config min.insync.replicas=2 \
    --config compression.type=lz4
```

| Throughput Target | Recommended Partitions |
|---|---|
| < 10 MB/s | 6 |
| 10–100 MB/s | 12–24 |
| > 100 MB/s | 50+ |

> ⚠️ You cannot reduce partitions after creation — only increase.

---

### 5.6 Security

```yaml
# docker-compose — enable SASL_SSL on broker
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: SASL_SSL:SASL_SSL,CONTROLLER:PLAINTEXT
KAFKA_LISTENERS: SASL_SSL://0.0.0.0:9092,CONTROLLER://broker:29093
KAFKA_SSL_KEYSTORE_LOCATION: /certs/kafka.keystore.jks
KAFKA_SSL_KEYSTORE_PASSWORD: changeit
KAFKA_SASL_ENABLED_MECHANISMS: SCRAM-SHA-512
KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL: SCRAM-SHA-512
```

```yaml
# Schema Registry with TLS
SCHEMA_REGISTRY_LISTENERS: https://0.0.0.0:8081
SCHEMA_REGISTRY_SSL_KEYSTORE_LOCATION: /certs/schema-registry.keystore.jks
SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL: SASL_SSL
```

```yaml
# application.yml — Spring Boot client with SASL_SSL
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
        username="app-user" password="${KAFKA_PASSWORD}";
      schema.registry.url: https://schema-registry:8081
      basic.auth.credentials.source: USER_INFO
      basic.auth.user.info: sr-user:${SR_PASSWORD}
```

---

### 5.7 Monitoring & Alerting

**Confluent Control Center** provides built-in monitoring. For Prometheus/Grafana:

```yaml
# Add JMX exporter sidecar to broker in docker-compose
broker:
  environment:
    KAFKA_JMX_PORT: 9101
    KAFKA_JMX_HOSTNAME: localhost
    EXTRA_ARGS: -javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=9102:/opt/jmx-exporter/kafka.yml
```

**Key metrics to alert on:**

| Metric | Alert Threshold | What It Means |
|---|---|---|
| `kafka_consumer_lag` | > 10,000 | Consumer falling behind |
| `kafka_server_UnderReplicatedPartitions` | > 0 | Broker down or replication broken |
| `kafka_controller_ActiveControllerCount` | != 1 | No active controller |
| `kafka_network_RequestHandlerAvgIdlePercent` | < 0.3 | Broker CPU saturated |
| Schema Registry `master_slave_role` | not `MASTER` | No SR leader elected |

**App metrics** available at `http://localhost:8085/actuator/prometheus` (already configured via `micrometer-registry-prometheus` in `pom.xml`).

---

### 5.8 Dead Letter Queue (DLQ) Pattern

```java
@RetryableTopic(
    attempts = "3",
    backoff = @Backoff(delay = 1000, multiplier = 2.0),
    dltStrategy = DltStrategy.FAIL_ON_ERROR
)
@KafkaListener(topics = "${app.kafka.topic}", groupId = "demo-group")
public void listen(String message) {
    processMessage(message);   // throws 3× → goes to my-topic-dlt
}

@DltHandler
public void handleDlt(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
    log.error("DLQ: failed message from topic={}: {}", topic, message);
    // store in DB, alert Slack, send to S3 for replay
}
```

---

### 5.9 Dev vs. Production Config Comparison

| Setting | This Project (Dev) | Production |
|---|---|---|
| Brokers | 1 | 3+ (across AZs) |
| Replication factor | 1 | 3 |
| Schema Registry | 1 instance | 3+ instances (HA) |
| Schema compatibility | default | `BACKWARD` enforced |
| `acks` (String) | default (1) | `all` |
| `acks` (Avro) | `all` ✅ | `all` ✅ |
| `enable.idempotence` (Avro) | `true` ✅ | `true` ✅ |
| `enable-auto-commit` | true | false |
| `ack-mode` | AUTO | `MANUAL_IMMEDIATE` |
| Security | `PLAINTEXT` | `SASL_SSL` |
| Control Center | trial (30 days) | licensed |
| Partitions | 3 | 12+ |
| Monitoring | Actuator + CC trial | Prometheus + Grafana + CC |
| DLQ | none | Required |

---

### 5.10 Production Docker Compose Template (3-Broker Confluent Platform)

```yaml
# docker-compose.prod.yml
services:
  broker-1:
    image: confluentinc/cp-kafka:7.7.0
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@broker-1:29093,2@broker-2:29093,3@broker-3:29093
      KAFKA_LISTENERS: PLAINTEXT://broker-1:29092,CONTROLLER://broker-1:29093,PLAINTEXT_HOST://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker-1:29092,PLAINTEXT_HOST://broker-1:9092
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
    volumes:
      - broker-1-data:/tmp/kraft-combined-logs

  broker-2:
    image: confluentinc/cp-kafka:7.7.0
    environment:
      KAFKA_NODE_ID: 2
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@broker-1:29093,2@broker-2:29093,3@broker-3:29093
      KAFKA_LISTENERS: PLAINTEXT://broker-2:29092,CONTROLLER://broker-2:29093,PLAINTEXT_HOST://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker-2:29092,PLAINTEXT_HOST://broker-2:9092
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
    volumes:
      - broker-2-data:/tmp/kraft-combined-logs

  broker-3:
    image: confluentinc/cp-kafka:7.7.0
    environment:
      KAFKA_NODE_ID: 3
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@broker-1:29093,2@broker-2:29093,3@broker-3:29093
      KAFKA_LISTENERS: PLAINTEXT://broker-3:29092,CONTROLLER://broker-3:29093,PLAINTEXT_HOST://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker-3:29092,PLAINTEXT_HOST://broker-3:9092
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
    volumes:
      - broker-3-data:/tmp/kraft-combined-logs

  schema-registry:
    image: confluentinc/cp-schema-registry:7.7.0
    environment:
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: broker-1:29092,broker-2:29092,broker-3:29092
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081

  control-center:
    image: confluentinc/cp-enterprise-control-center:7.7.0
    environment:
      CONTROL_CENTER_BOOTSTRAP_SERVERS: broker-1:29092,broker-2:29092,broker-3:29092
      CONTROL_CENTER_SCHEMA_REGISTRY_URL: http://schema-registry:8081
      CONTROL_CENTER_REPLICATION_FACTOR: 3
    ports:
      - "9021:9021"

volumes:
  broker-1-data:
  broker-2-data:
  broker-3-data:
```

---

## Project Structure Reference

```
kafka-practical/
├── docker-compose.yaml                          # Confluent Platform 7.7 stack
├── kafka/                                       # Legacy custom image (kept for reference)
│   ├── Dockerfile                               # No longer used
│   ├── server.properties                        # No longer used (env vars replace this)
│   └── entrypoint.sh                            # No longer used
├── src/
│   ├── main/
│   │   ├── avro/
│   │   │   └── KafkaMessage.avsc                # Avro schema → generates KafkaMessage.java
│   │   ├── java/com/example/kafka/
│   │   │   ├── KafkaApplication.java            # Spring Boot entry point
│   │   │   ├── config/
│   │   │   │   └── KafkaConfig.java             # Topics + Avro producer/consumer factories
│   │   │   ├── producer/
│   │   │   │   ├── MessageProducer.java          # String send() and sendWithKey()
│   │   │   │   └── AvroMessageProducer.java      # Avro send() and sendWithKey()
│   │   │   ├── consumer/
│   │   │   │   ├── MessageConsumer.java          # String @KafkaListener (2 groups)
│   │   │   │   └── AvroMessageConsumer.java      # Avro @KafkaListener
│   │   │   └── controller/
│   │   │       └── MessageController.java        # 4 endpoints: /send /send-keyed /avro/send /avro/send-keyed
│   │   └── resources/
│   │       └── application.yml                  # Port 8085, Schema Registry URL, topics
│   └── test/
├── pom.xml                                      # Confluent repo, Avro, Schema Registry deps
├── mvnw / mvnw.cmd                              # Maven wrapper
├── KAFKA_DOCS.md                                # This file
└── .gitignore                                   # Excludes target/, generated avro/, .idea/
```

### API Endpoints

| Method | URL | Description |
|---|---|---|
| `POST` | `/api/kafka/send?message=X` | Send String message (no key) |
| `POST` | `/api/kafka/send-keyed` | Send String message with key `{"key":"k","message":"v"}` |
| `POST` | `/api/kafka/avro/send?message=X` | Send Avro message (schema auto-registered) |
| `POST` | `/api/kafka/avro/send-keyed` | Send Avro message with key `{"key":"k","message":"v"}` |
| `GET`  | `http://localhost:8081/subjects` | List registered schemas (Schema Registry) |
| `GET`  | `http://localhost:9021` | Control Center web UI |
| `POST` | `http://localhost:8082/topics/{name}` | Produce via REST Proxy |
| `GET`  | `/actuator/health` | App health check |
| `GET`  | `/actuator/prometheus` | Prometheus metrics |
