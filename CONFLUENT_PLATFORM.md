# Confluent Platform — Enterprise Components
> Schema Registry · REST Proxy · Control Center
> Confluent Platform 7.7 · Spring Boot 3.3

---

## Table of Contents
1. [Schema Registry](#1-schema-registry)
2. [REST Proxy](#2-rest-proxy)
3. [Control Center](#3-control-center)

---

## 1. Schema Registry

### 1.1 What Is Schema Registry?

Schema Registry is a **centralized schema management service** that enforces a contract between producers and consumers. Instead of trusting that both sides agree on the message format, Schema Registry validates every schema before a message is published.

```
Without Schema Registry                 With Schema Registry
────────────────────────────────        ────────────────────────────────────────
Producer sends {"name": "Alice"}        Producer registers schema on first send
Consumer expects {"user": "Alice"}      Schema ID is embedded in every message
Result: silent deserialization error    Consumer fetches schema by ID → always correct
No history, no versioning               Full version history + compatibility checks
Schema change = silent data loss        Breaking change → rejected at publish time
```

---

### 1.2 How It Works

```
┌───────────────────────────────────────────────────────────────┐
│                    SCHEMA REGISTRY FLOW                       │
│                                                               │
│  Producer                                                     │
│     │                                                         │
│     │  1. First send: POST /subjects/my-topic-avro-value      │
│     │     ──────────────────────────────────────────────►     │
│     │                                          Schema Registry│
│     │  2. Schema stored, assigned schema ID = 1              │
│     │     ◄──────────────────────────────────────────────     │
│     │                                                         │
│     │  3. Message wire format:                                │
│     │     [ 0x00 | 0x00 0x00 0x00 0x01 | <avro bytes> ]      │
│     │       magic   schema ID = 1         payload             │
│     │                                                         │
│     │  4. Send to Kafka broker                                │
│     └──────────────────────────────────────────────────►      │
│                                              Kafka Broker     │
│                                                               │
│  Consumer                                                     │
│     │                                                         │
│     │  5. Poll message from broker                           │
│     │     ◄──────────────────────────────────────────────     │
│     │                                                         │
│     │  6. Read magic byte (0x00) → Confluent Avro format      │
│     │     Extract schema ID = 1                               │
│     │                                                         │
│     │  7. First time: GET /schemas/ids/1                      │
│     │     ──────────────────────────────────────────────►     │
│     │                                          Schema Registry│
│     │  8. Fetch schema, cache locally                        │
│     │     ◄──────────────────────────────────────────────     │
│     │                                                         │
│     │  9. Deserialize bytes → KafkaMessage object            │
└───────────────────────────────────────────────────────────────┘
```

Key points:
- The schema is registered **once** — all subsequent sends reuse the cached ID locally
- The consumer fetches the schema **once** per schema ID — then caches it
- Schema Registry stores schemas in a special Kafka topic: `_schemas`

---

### 1.3 Subjects & Naming Conventions

A **subject** is the named slot where a schema is stored. By default, Confluent uses the **TopicNameStrategy**:

| Strategy | Subject Name | Use Case |
|---|---|---|
| `TopicNameStrategy` (default) | `<topic>-key` or `<topic>-value` | One schema per topic |
| `RecordNameStrategy` | `<namespace>.<name>` | Same record type across multiple topics |
| `TopicRecordNameStrategy` | `<topic>-<namespace>.<name>` | Fine-grained per-topic-per-type control |

In this project:
```
Topic: my-topic-avro
Subject (value): my-topic-avro-value   ← KafkaMessage schema registered here
Subject (key):   my-topic-avro-key     ← only created if key is also Avro
```

---

### 1.4 Schema Compatibility Modes

Compatibility rules prevent breaking changes from reaching consumers. Set per-subject or globally.

```
BACKWARD (default recommended)
──────────────────────────────
New schema can read data written with OLD schema.
→ Safe to upgrade consumers first, then producers.
✅ Add optional field with default
✅ Delete a field
❌ Add required field (no default)
❌ Rename a field
❌ Change field type

FORWARD
───────
Old schema can read data written with NEW schema.
→ Safe to upgrade producers first, then consumers.
✅ Add required field
✅ Delete optional field
❌ Delete a required field

FULL
────
Both BACKWARD and FORWARD.
→ Safest — both old and new consumers/producers work simultaneously.
✅ Only add/remove optional fields with defaults

NONE
────
No checks. Any change accepted.
→ Dev/testing only. Never use in production.
```

---

### 1.5 Schema Registry REST API

The Schema Registry exposes a full REST API on **`http://localhost:8081`**.

#### Subjects

```bash
# List all registered subjects
curl http://localhost:8081/subjects
# ["my-topic-avro-value"]

# Get all versions of a subject
curl http://localhost:8081/subjects/my-topic-avro-value/versions
# [1, 2, 3]

# Get a specific version
curl http://localhost:8081/subjects/my-topic-avro-value/versions/1
# {
#   "subject": "my-topic-avro-value",
#   "version": 1,
#   "id": 1,
#   "schema": "{\"type\":\"record\",\"name\":\"KafkaMessage\",...}"
# }

# Get the latest version
curl http://localhost:8081/subjects/my-topic-avro-value/versions/latest

# Delete a subject (all versions) — dev only
curl -X DELETE http://localhost:8081/subjects/my-topic-avro-value

# Delete a specific version
curl -X DELETE http://localhost:8081/subjects/my-topic-avro-value/versions/1
```

#### Schemas

```bash
# Look up schema by global ID
curl http://localhost:8081/schemas/ids/1
# {"schema": "{\"type\":\"record\",...}"}

# List all schema types supported
curl http://localhost:8081/schemas/types
# ["JSON", "PROTOBUF", "AVRO"]
```

#### Compatibility

```bash
# Check if a new schema is compatible before registering
curl -X POST \
  http://localhost:8081/compatibility/subjects/my-topic-avro-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"KafkaMessage\",\"namespace\":\"com.example.kafka.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"key\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"content\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},{\"name\":\"source\",\"type\":[\"null\",\"string\"],\"default\":null}]}"
  }'
# {"is_compatible": true}   ✅  — safe to evolve

# Set compatibility mode for a subject
curl -X PUT \
  http://localhost:8081/config/my-topic-avro-value \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "BACKWARD"}'

# Get current compatibility setting for a subject
curl http://localhost:8081/config/my-topic-avro-value
# {"compatibilityLevel": "BACKWARD"}

# Get global default compatibility setting
curl http://localhost:8081/config
```

---

### 1.6 Evolving the Avro Schema Safely

**Scenario:** Add a new optional `source` field to `KafkaMessage`.

**Step 1 — Check compatibility first:**
```bash
curl -X POST http://localhost:8081/compatibility/subjects/my-topic-avro-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "{...new schema with source field...}"}'
# {"is_compatible": true}
```

**Step 2 — Update `KafkaMessage.avsc`:**
```json
{
  "type": "record",
  "name": "KafkaMessage",
  "namespace": "com.example.kafka.avro",
  "fields": [
    { "name": "id",        "type": "string" },
    { "name": "key",       "type": ["null", "string"], "default": null },
    { "name": "content",   "type": "string" },
    { "name": "timestamp", "type": { "type": "long", "logicalType": "timestamp-millis" } },
    { "name": "source",    "type": ["null", "string"], "default": null }
  ]
}
```

**Step 3 — Rebuild and restart Spring Boot:**
```powershell
.\mvnw.cmd spring-boot:run
```

On first send with the new schema, Schema Registry registers it as **version 2** (ID = 2).  
Consumers still running on version 1 can still read the messages — the `source` field defaults to `null`.

---

### 1.7 Schema Registry in Spring Boot (`KafkaConfig.java`)

```java
// Producer: KafkaAvroSerializer automatically registers schema on first send
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
props.put("schema.registry.url", "http://localhost:8081");

// Consumer: KafkaAvroDeserializer fetches schema by ID from registry
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
props.put("schema.registry.url", "http://localhost:8081");
props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
// SPECIFIC_AVRO_READER_CONFIG=true → returns KafkaMessage.class
// SPECIFIC_AVRO_READER_CONFIG=false → returns GenericRecord (schema-agnostic)
```

---

## 2. REST Proxy

### 2.1 What Is the REST Proxy?

The **REST Proxy** (port `8082`) exposes Kafka's produce and consume operations over plain HTTP/JSON. Any language or tool that can make an HTTP request can interact with Kafka — **no Kafka client library needed**.

```
Without REST Proxy              With REST Proxy
────────────────────────        ──────────────────────────────────────
Need Kafka client SDK           Plain curl / HTTP client is enough
Must handle TCP connections     REST Proxy manages Kafka connections
Language must have Kafka lib    Works with Python, Ruby, PHP, bash...
                                Great for webhooks, legacy systems, testing
```

---

### 2.2 Architecture

```
┌─────────────────────────────────────────────────────┐
│                    REST Proxy :8082                 │
│                                                     │
│  HTTP clients                                       │
│  ┌──────────┐   POST /topics/{name}                 │
│  │ curl     │ ──────────────────────► Kafka Broker  │
│  │ Postman  │                                       │
│  │ webhook  │   GET /consumers/{grp}/               │
│  │ PHP app  │ ◄────────────────────── Kafka Broker  │
│  └──────────┘                                       │
│                          │                          │
│                          ▼                          │
│                    Schema Registry                  │
│                    (for Avro messages)              │
└─────────────────────────────────────────────────────┘
```

---

### 2.3 Produce Messages

#### Produce JSON (no schema)

```bash
# Send a single message
curl -X POST http://localhost:8082/topics/my-topic \
  -H "Content-Type: application/vnd.kafka.json.v2+json" \
  -d '{"records": [{"value": "Hello from REST Proxy"}]}'

# Response:
# {
#   "offsets": [{"partition": 0, "offset": 5, "error_code": null, "error": null}],
#   "key_schema_id": null,
#   "value_schema_id": null
# }

# Send multiple messages in one request (batch)
curl -X POST http://localhost:8082/topics/my-topic \
  -H "Content-Type: application/vnd.kafka.json.v2+json" \
  -d '{
    "records": [
      {"value": "Message 1"},
      {"value": "Message 2"},
      {"value": "Message 3"}
    ]
  }'

# Send with a routing key
curl -X POST http://localhost:8082/topics/my-topic \
  -H "Content-Type: application/vnd.kafka.json.v2+json" \
  -d '{"records": [{"key": "user-1", "value": "Keyed message"}]}'

# Send to a specific partition
curl -X POST http://localhost:8082/topics/my-topic \
  -H "Content-Type: application/vnd.kafka.json.v2+json" \
  -d '{"records": [{"partition": 2, "value": "Goes to partition 2"}]}'
```

#### Produce Avro (with Schema Registry)

```bash
# Send Avro message — schema validated against Schema Registry
curl -X POST http://localhost:8082/topics/my-topic-avro \
  -H "Content-Type: application/vnd.kafka.avro.v2+json" \
  -d '{
    "value_schema": "{\"type\":\"record\",\"name\":\"KafkaMessage\",\"namespace\":\"com.example.kafka.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"key\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"content\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"long\"}]}",
    "records": [
      {
        "value": {
          "id": "rest-001",
          "key": null,
          "content": "Avro via REST Proxy",
          "timestamp": 1779638788000
        }
      }
    ]
  }'

# Response includes the schema ID registered (or looked up) in Schema Registry:
# {"offsets":[...],"key_schema_id":null,"value_schema_id":1}
```

---

### 2.4 Consume Messages

Consuming via REST Proxy requires **three steps**: create a consumer instance, subscribe to topics, then fetch records.

```bash
# Step 1: Create a consumer instance in a group
curl -X POST http://localhost:8082/consumers/my-rest-group \
  -H "Content-Type: application/vnd.kafka.v2+json" \
  -d '{
    "name": "my-consumer-1",
    "format": "json",
    "auto.offset.reset": "earliest"
  }'
# Response: {"instance_id":"my-consumer-1","base_uri":"http://rest-proxy:8082/consumers/my-rest-group/instances/my-consumer-1"}

# Step 2: Subscribe to topics
curl -X POST http://localhost:8082/consumers/my-rest-group/instances/my-consumer-1/subscription \
  -H "Content-Type: application/vnd.kafka.v2+json" \
  -d '{"topics": ["my-topic"]}'

# Step 3: Fetch records (poll)
curl http://localhost:8082/consumers/my-rest-group/instances/my-consumer-1/records \
  -H "Accept: application/vnd.kafka.json.v2+json"
# [{"topic":"my-topic","key":null,"value":"Hello","partition":0,"offset":0}]

# Step 4: Commit offsets (mark as processed)
curl -X POST \
  http://localhost:8082/consumers/my-rest-group/instances/my-consumer-1/offsets \
  -H "Content-Type: application/vnd.kafka.v2+json" \
  -d '{"offsets": [{"topic": "my-topic", "partition": 0, "offset": 1}]}'

# Step 5: Delete consumer instance when done (release server-side resources)
curl -X DELETE \
  http://localhost:8082/consumers/my-rest-group/instances/my-consumer-1 \
  -H "Content-Type: application/vnd.kafka.v2+json"
```

---

### 2.5 Inspect Topics & Metadata

```bash
# List all topics
curl http://localhost:8082/topics
# ["my-topic","my-topic-avro","_schemas"]

# Get topic metadata (partitions, replicas, leader)
curl http://localhost:8082/topics/my-topic
# {"name":"my-topic","configs":{...},"partitions":[{"partition":0,"leader":1,...},...]}

# Get partition metadata
curl http://localhost:8082/topics/my-topic/partitions
curl http://localhost:8082/topics/my-topic/partitions/0
```

---

### 2.6 Content Types Reference

The REST Proxy uses custom MIME types to indicate the message format:

| Content-Type | Format | Use Case |
|---|---|---|
| `application/vnd.kafka.json.v2+json` | JSON records | General purpose, easiest to use |
| `application/vnd.kafka.avro.v2+json` | Avro + Schema Registry | Schema-validated structured data |
| `application/vnd.kafka.binary.v2+json` | Base64-encoded raw bytes | Binary payloads |
| `application/vnd.kafka.v2+json` | Control operations | Consumer create/delete/subscribe |

---

### 2.7 When to Use REST Proxy

| Scenario | Use REST Proxy? |
|---|---|
| Quick manual testing / debugging | ✅ Yes — simplest way to push a test message |
| Webhook receiver that forwards to Kafka | ✅ Yes — no Kafka SDK needed in the webhook service |
| Legacy PHP/Ruby app without Kafka client | ✅ Yes |
| High-throughput microservice (>10k msg/s) | ❌ No — use native Kafka client (lower overhead) |
| Long-running consumer service | ❌ No — REST consumer state is ephemeral |
| Spring Boot producer/consumer | ❌ No — native `spring-kafka` is more efficient |

---

## 3. Control Center

### 3.1 What Is Control Center?

**Confluent Control Center** (port `9021`) is an enterprise web UI for operating, monitoring, and managing the entire Confluent Platform stack. It gives visibility into brokers, topics, consumer lag, schemas, and cluster health — all without running CLI commands.

> 🔑 **Licensing:** Free for 30 days, then requires a Confluent Platform license.  
> For an unlimited free alternative, use [kafka-ui](https://github.com/provectuslabs/kafka-ui) (`provectuslabs/kafka-ui`).

---

### 3.2 Accessing Control Center

```
URL: http://localhost:9021
```

Control Center takes **~60–90 seconds** to fully boot after `docker-compose up`. If you see a loading screen or blank page, wait and refresh.

---

### 3.3 Navigating the UI

```
Control Center
└── Home
    └── Cluster Overview
        ├── Brokers          ← Health, disk, network I/O per broker
        ├── Topics           ← All topics, message rates, storage
        ├── Consumers        ← Consumer groups and lag
        ├── Schema Registry  ← View, search and manage schemas
        ├── ksqlDB           ← Stream processing (if ksqlDB is running)
        └── Connect          ← Kafka Connect connectors (if running)
```

---

### 3.4 Topics Tab

**Navigate:** Cluster → Topics

```
Topics list view
┌────────────────────────────────────────┬────────────┬──────────┬──────────┐
│ Topic Name                             │ Partitions │ Replicas │ Msgs/sec │
├────────────────────────────────────────┼────────────┼──────────┼──────────┤
│ my-topic                               │ 3          │ 1        │ 0.5      │
│ my-topic-avro                          │ 3          │ 1        │ 0.2      │
│ my-topic-uppercase                     │ 3          │ 1        │ 0.5      │  ← Streams output
│ my-topic-wordcount                     │ 3          │ 1        │ 0.2      │  ← Streams output
│ kafka-streams-demo-...-repartition     │ 3          │ 1        │ 0        │  ← Streams internal
│ kafka-streams-demo-word-count-store-.. │ 1          │ 1        │ 0        │  ← State store backup
│ _schemas                               │ 1          │ 1        │ 0        │
└────────────────────────────────────────┴────────────┴──────────┴──────────┘
```

> **Kafka Streams internal topics** are managed automatically and prefixed with the
> `application-id` (`kafka-streams-demo`). Do not delete them — they are used to back up
> the word count state store and to route repartitioned records.

**Click a topic → Messages tab** to browse messages:
- Filter by partition, offset range, or timestamp
- View message key, value, headers, and metadata
- Jump to a specific offset

**Click a topic → Configuration tab** to view/edit:
- `retention.ms`, `cleanup.policy`, `compression.type`, `min.insync.replicas`

---

### 3.5 Consumer Groups Tab

**Navigate:** Cluster → Consumers

Shows every consumer group with their **lag** per partition — the most important operational metric.

```
Consumer Groups
┌─────────────────────────┬─────────────────────┬────────┬────────┐
│ Group ID                │ Topic               │ Lag    │ Status │
├─────────────────────────┼─────────────────────┼────────┼────────┤
│ demo-group              │ my-topic            │ 0      │ ✅     │
│ demo-group-advanced     │ my-topic            │ 0      │ ✅     │
│ demo-group-avro         │ my-topic-avro       │ 0      │ ✅     │
│ kafka-streams-demo      │ my-topic            │ 0      │ ✅     │  ← Kafka Streams
└─────────────────────────┴─────────────────────┴────────┴────────┘
```

> **kafka-streams-demo** is the Kafka Streams application consuming from `my-topic`.
> Its `application-id` (set in `KafkaStreamsConfig.java`) doubles as the consumer group ID.
> It processes each message through two branches: uppercase transformation and word count aggregation.

**Click a group** to see per-partition breakdown:
- Which consumer instance owns each partition
- `Current Offset`, `Log End Offset`, and `Lag` per partition
- Consumer host and client ID

> ⚠️ **Lag > 0** means your consumer is falling behind. Investigate slow processing or scale up consumers.

---

### 3.6 Schema Registry Tab

**Navigate:** Cluster → Schema Registry

```
Schemas list
┌──────────────────────────────┬─────────┬──────────────┐
│ Subject                      │ Version │ Format       │
├──────────────────────────────┼─────────┼──────────────┤
│ my-topic-avro-value          │ 1       │ AVRO         │
└──────────────────────────────┴─────────┴──────────────┘
```

**Click a schema** to:
- View the full schema definition (pretty-printed JSON)
- Browse all versions with diffs between them
- See which topics use this schema
- Check and change the compatibility mode

---

### 3.7 Brokers Tab

**Navigate:** Cluster → Brokers

Shows per-broker metrics in real-time:

| Metric | What It Tells You |
|---|---|
| **Disk Usage** | How full each broker's storage is |
| **Network I/O** | Bytes in/out per second |
| **Partition Count** | How many partitions this broker leads |
| **Under-replicated** | Partitions where replicas are lagging (should be 0) |
| **Request Latency** | P99 produce/fetch latency |

---

### 3.8 Alerts & Triggers

Control Center can send alerts when thresholds are breached.

**Navigate:** Cluster → Alerts → Edit Triggers

Built-in trigger types:

| Trigger | Recommended Threshold |
|---|---|
| Consumer lag | > 1,000 messages |
| Under-replicated partitions | > 0 |
| Broker offline | Any |
| Cluster disk usage | > 80% |
| Request latency (P99) | > 500ms |

**Navigate:** Alerts → Edit Actions to configure notification destinations:
- Email
- Webhook (Slack, PagerDuty, etc.)

---

### 3.9 Creating a Topic via Control Center

1. Navigate to **Cluster → Topics → + Add a topic**
2. Set name, partition count, replication factor
3. Expand **Advanced settings** to set retention, compression, min ISR
4. Click **Create with defaults** or **Customize settings**

---

### 3.10 Control Center vs CLI vs REST API

| Task | Control Center | CLI | REST API |
|---|---|---|---|
| Browse messages | ✅ Visual, filterable | ✅ `console-consumer` | ✅ REST Proxy |
| Monitor lag | ✅ Real-time dashboard | ✅ `consumer-groups.sh` | ❌ |
| Create topic | ✅ UI form | ✅ `kafka-topics.sh` | ✅ Admin API |
| Manage schemas | ✅ UI + diff view | ❌ | ✅ SR REST API |
| Set alerts | ✅ Built-in | ❌ | ❌ |
| Automation / scripting | ❌ | ✅ | ✅ |
| Works without browser | ❌ | ✅ | ✅ |

---

## Quick Reference

### Service URLs

| Service | URL | Purpose |
|---|---|---|
| **Kafka Broker** | `localhost:9092` | Producer/consumer bootstrap |
| **Schema Registry** | `http://localhost:8081` | Schema CRUD + compatibility |
| **REST Proxy** | `http://localhost:8082` | HTTP produce/consume |
| **Control Center** | `http://localhost:9021` | Web UI |
| **Spring Boot App** | `http://localhost:8085` | Application API |

---

### Schema Registry Cheatsheet

```bash
# List subjects
curl http://localhost:8081/subjects

# Get latest schema
curl http://localhost:8081/subjects/my-topic-avro-value/versions/latest

# Check schema compatibility
curl -X POST http://localhost:8081/compatibility/subjects/my-topic-avro-value/versions/latest \
     -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     -d '{"schema": "<new-schema-json>"}'

# Set compatibility mode
curl -X PUT http://localhost:8081/config/my-topic-avro-value \
     -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     -d '{"compatibility": "BACKWARD"}'
```

---

### REST Proxy Cheatsheet

```bash
# Produce JSON message
curl -X POST http://localhost:8082/topics/my-topic \
     -H "Content-Type: application/vnd.kafka.json.v2+json" \
     -d '{"records": [{"value": "Hello"}]}'

# Produce with key
curl -X POST http://localhost:8082/topics/my-topic \
     -H "Content-Type: application/vnd.kafka.json.v2+json" \
     -d '{"records": [{"key": "user-1", "value": "Hello"}]}'

# List topics
curl http://localhost:8082/topics

# Topic metadata
curl http://localhost:8082/topics/my-topic
```

---

### Control Center Cheatsheet

```
Open:         http://localhost:9021
Topics:       Cluster → Topics → [topic] → Messages
Consumer lag: Cluster → Consumers → [group]
Schemas:      Cluster → Schema Registry → [subject]
Brokers:      Cluster → Brokers
Alerts:       Cluster → Alerts → Edit Triggers
```
