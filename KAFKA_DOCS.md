# Kafka Documentation
> Spring Boot 3.3 + **Confluent Platform 7.7** · Avro + Schema Registry · Kafka Streams · KRaft Mode · Docker

---

## Table of Contents
1. [How Kafka Works](#1-how-kafka-works)
   - 1.1 [Confluent Platform Architecture](#11-confluent-platform-architecture)
   - 1.2 [Key Components](#12-key-components)
   - 1.3 [String Message Flow](#13-string-message-flow)
   - 1.4 [Avro Message Flow (Schema Registry)](#14-avro-message-flow-schema-registry)
   - 1.5 [Schema Registry — Why It Matters](#15-schema-registry--why-it-matters)
   - 1.6 [Kafka Streams — Stream Processing](#16-kafka-streams--stream-processing)
   - 1.7 [Kafka Streams vs Regular Consumer](#17-kafka-streams-vs-regular-consumer)
   - 1.8 [Partitions & Consumer Groups](#18-partitions--consumer-groups)
   - 1.9 [KRaft Mode (No Zookeeper)](#19-kraft-mode-no-zookeeper)
2. [Kafka Configuration](#2-kafka-configuration)
   - 2.1 [Port Map](#21-port-map)
   - 2.2 [Confluent Platform Stack — docker-compose.yaml](#22-confluent-platform-stack--docker-composeyaml)
   - 2.3 [Spring Boot Configuration — application.yml](#23-spring-boot-configuration--applicationyml)
   - 2.4 [Avro Schema — KafkaMessage.avsc](#24-avro-schema--kafkamessageavsc)
   - 2.5 [Avro Kafka Beans — KafkaConfig.java](#25-avro-kafka-beans--kafkaconfigjava)
   - 2.6 [Kafka Streams Config — KafkaStreamsConfig.java](#26-kafka-streams-config--kafkastreamsconfigjava)
   - 2.7 [Streams Topology — MessageStreamsTopology.java](#27-streams-topology--messagestreams-topologyjava)
   - 2.8 [Topic Configuration — KafkaConfig.java](#28-topic-configuration--kafkaconfigjava)
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
│            │                   │                    │                       │
│  ┌─────────▼──────────────────▼────────────────────▼────────────────┐      │
│  │                    Spring Boot App  :8085                         │      │
│  │                                                                   │      │
│  │  POST /api/kafka/send              → MessageProducer (String)    │      │
│  │  POST /api/kafka/avro/send         → AvroMessageProducer         │      │
│  │  GET  /api/kafka/streams/wordcount → WordCountQueryService       │      │
│  │                                                                   │      │
│  │  ┌─────────────────────────────────────────────────────────┐    │      │
│  │  │  Kafka Streams (embedded in-process)                    │    │      │
│  │  │                                                         │    │      │
│  │  │  my-topic ──► uppercase branch ──► my-topic-uppercase  │    │      │
│  │  │            └► word count branch ──► my-topic-wordcount  │    │      │
│  │  │                  (RocksDB state store — queryable)      │    │      │
│  │  └─────────────────────────────────────────────────────────┘    │      │
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
| **Kafka Streams** | Embedded stream processor with state store | `MessageStreamsTopology.java` |
| **Word Count Query** | Interactive query service against local RocksDB | `WordCountQueryService.java` |
| **Topics** | Named channels for messages | `my-topic`, `my-topic-avro`, `my-topic-uppercase`, `my-topic-wordcount` |
| **Partition** | Ordered, immutable log within a topic | 3 partitions per topic |
| **Consumer Group** | Group of consumers sharing partition load | `demo-group`, `demo-group-avro`, `kafka-streams-demo` |

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
  Written to Docker volume: broker-data:/var/lib/kafka/data
  Retained for 7 days (default)

Step 5: Consumers receive
──────────────────────────
  demo-group          → MessageConsumer.listen()             → logs value
  demo-group-advanced → MessageConsumer.listenWithMetadata() → logs full record

Step 6: Kafka Streams also processes
──────────────────────────────────────
  kafka-streams-demo  → uppercase + word count topology (see section 1.6)
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
    timestamp: 1779638788106   ← Unix epoch millis (plain long)
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
─────────────────────────        ─────────────────────────────────
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

### 1.6 Kafka Streams — Stream Processing

Kafka Streams is a **client library** embedded directly inside your Spring Boot application (no separate cluster needed). It continuously reads from input topics, processes records through a topology, and writes results to output topics — all within the same JVM process.

#### Topology in This Project

```
my-topic (source)
    │
    ├── Branch 1: Stateless Transformation
    │       mapValues(String::toUpperCase)
    │       └──────────────────────────────► my-topic-uppercase
    │
    └── Branch 2: Stateful Word Count
            flatMapValues(split by whitespace)
            filter(non-empty words)
            selectKey(word)         ← triggers internal repartition
            groupByKey
            count(Materialized as "word-count-store")   ← RocksDB
            toStream
            └──────────────────────────────► my-topic-wordcount
```

#### Key Concepts

| Concept | Description |
|---|---|
| **KStream** | Unbounded stream of key-value records. Every record is a new event. |
| **KTable** | Changelog stream — each key has exactly one value (latest wins). Like a database table. |
| **Stateless op** | `map`, `filter`, `mapValues` — no memory of past records |
| **Stateful op** | `count`, `aggregate`, `join` — requires state store to remember history |
| **State Store** | Local RocksDB database inside the Streams app. Backed by a Kafka changelog topic for fault tolerance. |
| **Interactive Query** | Read directly from the local state store via code — no need to publish a query to Kafka |
| **Repartition** | Changing the key of a record triggers an automatic internal re-routing topic so records with the same new key land on the same partition |

#### Interactive Queries — No Kafka Round-Trip

After messages are processed, the word counts live in a local RocksDB store named `word-count-store`. You can query it directly via the REST API:

```
GET /api/kafka/streams/wordcount?word=hello
    │
    └── WordCountQueryService.getCount("hello")
             │
             └── KafkaStreams.store("word-count-store", keyValueStore())
                      │
                      └── RocksDB.get("hello") → 3L    ← microsecond lookup
                                                         no Kafka network call
```

---

### 1.7 Kafka Streams vs Regular Consumer

| | Regular @KafkaListener | Kafka Streams |
|---|---|---|
| **Best for** | Simple consume & process | Transformations, aggregations, joins |
| **State** | Stateless (unless you manage it yourself) | Built-in RocksDB state store |
| **Output** | Side effects (DB writes, API calls) | Writes back to Kafka topics |
| **Exactly-once** | Requires manual ACK | Built-in EOS support |
| **Query** | Not supported | Interactive queries on local state |
| **Topology** | None | DAG of operators compiled into a processing graph |
| **This project** | `MessageConsumer`, `AvroMessageConsumer` | `MessageStreamsTopology` |

---

### 1.8 Partitions & Consumer Groups

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

Kafka Streams also acts as a consumer group:
  Group: "kafka-streams-demo"   ← set by StreamsConfig.APPLICATION_ID_CONFIG
  Reads: my-topic (all 3 partitions)
  Writes: my-topic-uppercase, my-topic-wordcount
```

---

### 1.9 KRaft Mode (No Zookeeper)

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
| `29092` | Kafka Broker (internal) | Used by Schema Registry, REST Proxy, and Kafka Streams inside Docker network |
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
      KAFKA_LOG_DIRS: /var/lib/kafka/data        # Pre-configured with correct permissions
    ports:
      - "9092:9092"    # Expose only the host-facing listener
      - "9101:9101"    # JMX metrics
    volumes:
      - broker-data:/var/lib/kafka/data          # Persist messages across restarts
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

    # ── Kafka Streams ───────────────────────────────────────────────
    streams:
      application-id: kafka-streams-demo   # doubles as consumer group ID
      properties:
        default.key.serde: org.apache.kafka.common.serialization.Serdes$StringSerde
        default.value.serde: org.apache.kafka.common.serialization.Serdes$StringSerde

    # ── Shared Confluent properties (used by Avro + Streams beans) ──
    properties:
      schema.registry.url: http://localhost:8081

app:
  kafka:
    topic: my-topic                   # String source topic (also Streams input)
    avro-topic: my-topic-avro         # Avro topic
    uppercase-topic: my-topic-uppercase  # Streams output: uppercase transformation
    wordcount-topic: my-topic-wordcount  # Streams output: word count aggregation
```

---

### 2.4 Avro Schema — `src/main/avro/KafkaMessage.avsc`

```json
{
  "type": "record",
  "name": "KafkaMessage",
  "namespace": "com.example.kafka.avro",
  "doc": "Schema for messages published to Kafka via Confluent Schema Registry",
  "fields": [
    { "name": "id",        "type": "string",
      "doc": "Unique message identifier (UUID)" },
    { "name": "key",       "type": ["null", "string"], "default": null,
      "doc": "Optional routing key — determines target partition" },
    { "name": "content",   "type": "string",
      "doc": "The message payload" },
    { "name": "timestamp", "type": "long",
      "doc": "Unix epoch in milliseconds when the message was created" }
  ]
}
```

> **Why `timestamp` is a plain `long` (not `logicalType: timestamp-millis`)**
>
> `avro-maven-plugin` 1.11.3 generates broken code when `logicalType: timestamp-millis` is used:
> it creates `setTimestamp(Instant)` that tries to assign an `Instant` to a `long` field, causing
> a compile error. Using a plain `long` avoids this and works correctly — the timestamp is stored
> as Unix epoch milliseconds and accessed via `Instant.now().toEpochMilli()` in the producer.

The `avro-maven-plugin` generates `KafkaMessage.java` automatically during `mvn compile` into
`src/main/java/com/example/kafka/avro/`. The generated class is excluded from git via `.gitignore`.

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
    props.put(ProducerConfig.RETRIES_CONFIG, 3);
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

> **Why are `@Primary` String beans required?**
>
> When you define any `KafkaTemplate` bean (e.g. `avroKafkaTemplate`), Spring Boot's
> `@ConditionalOnMissingBean(KafkaTemplate.class)` auto-configuration backs off and stops
> creating the default String `KafkaTemplate`. The `@Primary` annotation on the explicit
> String beans tells Spring "this is the default to inject when no `@Qualifier` is specified",
> restoring the expected behaviour for `MessageProducer`.

---

### 2.6 Kafka Streams Config — `KafkaStreamsConfig.java`

`@EnableKafkaStreams` registers a `StreamsBuilderFactoryBean` and a `StreamsBuilder` in the Spring
context. Any `@Bean` method that accepts a `StreamsBuilder` parameter contributes processing nodes
to the shared topology before the Streams application starts.

```java
@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    /**
     * Bean name DEFAULT_STREAMS_CONFIG_BEAN_NAME is the magic name Spring Kafka
     * looks for to override default Streams settings.
     */
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfiguration() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,      "kafka-streams-demo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,   bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,   Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put("schema.registry.url",                    schemaRegistryUrl);
        // Flush state stores every second (good for dev; raise in production)
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        // Single-broker dev setup — internal changelog topics use RF=1
        props.put("replication.factor", 1);
        return new KafkaStreamsConfiguration(props);
    }
}
```

**Key settings:**

| Property | Value | Why |
|---|---|---|
| `application.id` | `kafka-streams-demo` | Unique app ID; also used as Kafka consumer group ID and state store directory prefix |
| `commit.interval.ms` | `1000` | How often state store changes are flushed to the changelog topic. Lower = more up-to-date interactive query results |
| `replication.factor` | `1` | Internal Kafka Streams topics (changelog, repartition) use this replication factor. Must be ≤ number of brokers |
| Default Serdes | `StringSerde` | Default key and value (de)serializers for stream operations |

---

### 2.7 Streams Topology — `MessageStreamsTopology.java`

The full topology is built in a single `@Bean` method that receives the auto-wired `StreamsBuilder`.
Both branches share the same source stream so `my-topic` is only subscribed to once.

```java
@Bean
public KStream<String, String> messageStream(StreamsBuilder builder) {

    KStream<String, String> source = builder.stream(inputTopic);  // my-topic

    // ── Branch 1: Stateless uppercase transformation ─────────────────────
    source
        .mapValues(String::toUpperCase)
        .to(uppercaseTopic);                      // → my-topic-uppercase

    // ── Branch 2: Stateful word count ────────────────────────────────────
    source
        .flatMapValues(v -> Arrays.asList(v.toLowerCase().split("\\W+")))
        .filter((k, word) -> !word.isEmpty())
        .selectKey((k, word) -> word)             // ← triggers repartition
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .count(Materialized
                .<String, Long, KeyValueStore<Bytes, byte[]>>as("word-count-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()))
        .toStream()
        .to(wordcountTopic, Produced.with(Serdes.String(), Serdes.Long()));
                                                  // → my-topic-wordcount

    return source;
}
```

**Why `selectKey` causes a repartition:**

`flatMapValues` keeps the original Kafka key (e.g., `null` or `"user-1"`). For word counting,
we need all occurrences of the same word to land on the same partition (so the count is accurate).
`selectKey` re-keys each record to the word itself. Kafka Streams detects the key change and
automatically creates an internal repartition topic (`kafka-streams-demo-...-repartition`) to
route records to the correct partition before aggregation.

---

### 2.8 Topic Configuration — `KafkaConfig.java`

```java
// Source topics
@Bean public NewTopic myTopic() {
    return TopicBuilder.name("my-topic").partitions(3).replicas(1).build();
}
@Bean public NewTopic myAvroTopic() {
    return TopicBuilder.name("my-topic-avro").partitions(3).replicas(1).build();
}

// Kafka Streams output topics (auto-created, but explicit beans ensure correct config)
@Bean public NewTopic myUppercaseTopic() {
    return TopicBuilder.name("my-topic-uppercase").partitions(3).replicas(1).build();
}
@Bean public NewTopic myWordcountTopic() {
    return TopicBuilder.name("my-topic-wordcount").partitions(3).replicas(1).build();
}
```

**All topics in this project:**

| Topic | Type | Source | Consumer Group |
|---|---|---|---|
| `my-topic` | String | REST API → `MessageProducer` | `demo-group`, `demo-group-advanced`, `kafka-streams-demo` |
| `my-topic-avro` | Avro | REST API → `AvroMessageProducer` | `demo-group-avro` |
| `my-topic-uppercase` | String | Kafka Streams (stateless) | — (Streams output) |
| `my-topic-wordcount` | Long values | Kafka Streams (stateful) | — (Streams output) |

**Partition count guidelines:**

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

---

### 3.3 Topic Management

```bash
# ── List all topics ─────────────────────────────────────────────
kafka-topics.sh --bootstrap-server localhost:9092 --list
# my-topic
# my-topic-avro
# my-topic-uppercase
# my-topic-wordcount
# kafka-streams-demo-...-repartition   ← Streams internal repartition topic
# kafka-streams-demo-word-count-store-changelog  ← Streams state store backup
# _schemas                    ← Schema Registry internal topic
# _confluent-monitoring       ← Control Center internal topic

# ── Describe a topic ────────────────────────────────────────────
kafka-topics.sh --bootstrap-server localhost:9092 \
    --describe --topic my-topic

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

# ── Read Streams uppercase output ───────────────────────────────
kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic my-topic-uppercase \
    --from-beginning \
    --property print.key=true \
    --property print.partition=true

# ── Read Streams word count output (Long-valued) ─────────────────
kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic my-topic-wordcount \
    --from-beginning \
    --property print.key=true \
    --value-deserializer org.apache.kafka.common.serialization.LongDeserializer

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
# (type message, press Enter — Kafka Streams will also process it)

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
    --property value.schema='{"type":"record","name":"KafkaMessage","namespace":"com.example.kafka.avro","fields":[{"name":"id","type":"string"},{"name":"key","type":["null","string"],"default":null},{"name":"content","type":"string"},{"name":"timestamp","type":"long"}]}'
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
# kafka-streams-demo           ← Kafka Streams consumer group

# ── Describe a group (offsets + lag) ────────────────────────────
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe --group demo-group

# GROUP        TOPIC     PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
# demo-group   my-topic  0          5               5               0  ✅
# demo-group   my-topic  1          2               2               0
# demo-group   my-topic  2          3               3               0

# ── Describe Kafka Streams consumer group ────────────────────────
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe --group kafka-streams-demo

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
    -d '{"schema": "{\"type\":\"record\",\"name\":\"KafkaMessage\",\"namespace\":\"com.example.kafka.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"key\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"content\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"source\",\"type\":[\"null\",\"string\"],\"default\":null}]}"}'
# {"is_compatible":true}

# Delete a subject (dev only — irreversible in production)
curl -X DELETE http://localhost:8081/subjects/my-topic-avro-value
```

---

### 3.8 Debug Kafka Streams

```powershell
# Check Streams app state via Spring Boot actuator
curl http://localhost:8085/actuator/health
# "kafkaStreams": {"status": "UP", "details": {"state": "RUNNING"}}

# Watch Streams-related logs in Spring Boot output
# Look for lines like:
# [Streams][Uppercase] 'hello world' → 'HELLO WORLD'
# [Streams][WordCount] 'hello' = 2
```

```bash
# Inside broker container — list all Streams-managed topics
kafka-topics.sh --bootstrap-server localhost:9092 --list | grep kafka-streams

# kafka-streams-demo-word-count-store-changelog  ← state store backup topic
# kafka-streams-demo-...-repartition             ← internal repartition for word count
# kafka-streams-demo-...-repartition-repartition ← (if selectKey triggered twice)

# Describe the state store changelog topic
kafka-topics.sh --bootstrap-server localhost:9092 \
    --describe --topic kafka-streams-demo-word-count-store-changelog
```

**Reset Kafka Streams state (dev — wipes all word counts):**
```powershell
# 1. Stop the Spring Boot app first
# 2. Reset the Streams application offsets and delete internal topics
docker-compose exec broker kafka-streams-application-reset.sh \
    --bootstrap-servers localhost:9092 \
    --application-id kafka-streams-demo \
    --input-topics my-topic \
    --intermediate-topics kafka-streams-demo-...-repartition

# 3. Restart the Spring Boot app — state store is rebuilt from scratch
```

**State store directory** (on the host, inside the JVM process):
```
%TEMP%\kafka-streams\kafka-streams-demo\   (Windows default)
/tmp/kafka-streams/kafka-streams-demo/     (Linux default)
```

---

### 3.9 Common Problems & Fixes

| Symptom | Cause | Fix |
|---|---|---|
| `Connection refused localhost:9092` | Broker not running | `docker-compose up -d` |
| `LEADER_NOT_AVAILABLE` on startup | Topic being created, brief delay | Wait 2–3s, retry |
| `Schema not found` on consumer start | Schema Registry not ready | Wait for `service_healthy` check; check `docker-compose ps` |
| `Port 8081 conflict` | Schema Registry vs Spring Boot both on 8081 | Spring Boot is on port **8085** |
| Consumer not receiving messages | Wrong `group-id` or `auto-offset-reset` | Check group name; use `earliest`; or reset offsets |
| `GET /streams/wordcount` returns `503` | Kafka Streams not RUNNING yet | Wait 5–10s after app start and retry |
| Word count returns `0` for known word | State store not committed yet | Wait 1s (commit.interval.ms=1000) then retry |
| Streams internal topics missing | Streams app never started | Check for `@EnableKafkaStreams` on `KafkaStreamsConfig` |
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
[Streams][Uppercase] 'HelloKafka' → 'HELLOKAFKA'
[Streams][WordCount] 'hellokafka' = 1
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
[Streams][Uppercase] 'Hello from user 1' → 'HELLO FROM USER 1'
[Streams][WordCount] 'hello' = 1
[Streams][WordCount] 'from' = 1
[Streams][WordCount] 'user' = 1
```

---

### 4.4 Test 3 — Avro Message (Schema Registry)

```bash
# Send Avro message — schema is auto-registered on first send
curl -X POST "http://localhost:8085/api/kafka/avro/send?message=HelloAvro"
```

**Expected app logs:**
```
[Avro] Sent id=550e8400-... content='HelloAvro' -> partition=1, offset=0
[Avro] Received → topic=my-topic-avro, partition=1, offset=0 | id=550e8400-..., key=null, content='HelloAvro', sentAt=1779638788106
```

**Verify the schema was registered:**
```powershell
Invoke-RestMethod http://localhost:8081/subjects
# ["my-topic-avro-value"]

Invoke-RestMethod http://localhost:8081/subjects/my-topic-avro-value/versions/1
```

---

### 4.5 Test 4 — Avro Keyed Message

```bash
curl -X POST http://localhost:8085/api/kafka/avro/send-keyed \
    -H "Content-Type: application/json" \
    -d '{"key":"user-1","message":"Avro from user 1"}'
```

---

### 4.6 Test 5 — Kafka Streams Word Count

Send a few messages first, then query the word count state store:

```bash
# Step 1: Send messages with repeated words
curl -X POST "http://localhost:8085/api/kafka/send?message=hello world"
curl -X POST "http://localhost:8085/api/kafka/send?message=hello kafka"
curl -X POST "http://localhost:8085/api/kafka/send?message=kafka streams"

# Step 2: Wait ~1 second for state store to commit (commit.interval.ms=1000)

# Step 3: Query a specific word count
curl "http://localhost:8085/api/kafka/streams/wordcount?word=hello"
# → {"word":"hello","count":2}

curl "http://localhost:8085/api/kafka/streams/wordcount?word=kafka"
# → {"word":"kafka","count":2}

curl "http://localhost:8085/api/kafka/streams/wordcount?word=world"
# → {"word":"world","count":1}

# Step 4: Dump all word counts at once
curl "http://localhost:8085/api/kafka/streams/wordcount/all"
# → {"hello":2,"world":1,"kafka":2,"streams":1}
```

> **Note:** The word count is cumulative — it keeps growing as you send more messages.
> Each new send adds to the existing count in the RocksDB state store.

---

### 4.7 Test 6 — Verify Uppercase Output Topic

```bash
# Inside broker container
docker-compose exec broker bash

kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic my-topic-uppercase \
    --from-beginning \
    --property print.key=true
# Output: null	HELLO WORLD
#         null	HELLO KAFKA
#         null	KAFKA STREAMS
```

---

### 4.8 Test 7 — REST Proxy (No Kafka Client Needed)

```bash
# Produce a message via REST Proxy
curl -X POST http://localhost:8082/topics/my-topic \
    -H "Content-Type: application/vnd.kafka.json.v2+json" \
    -d '{"records":[{"value":"Hello from REST Proxy"}]}'
# Response: {"offsets":[{"partition":0,"offset":2,"error_code":null,"error":null}]}
# Note: Kafka Streams will also pick this up and word-count it
```

---

### 4.9 Test 8 — Control Center Web UI

1. Open **http://localhost:9021** in your browser
2. Select your cluster → **Topics** → you will see all topics including:
   - `my-topic`, `my-topic-avro` — source topics
   - `my-topic-uppercase`, `my-topic-wordcount` — Streams output topics
   - `kafka-streams-demo-...-repartition` — Streams internal repartition topic
   - `kafka-streams-demo-word-count-store-changelog` — State store backup topic
3. Go to **Messages** tab on any topic → browse messages with partition, offset, timestamp
4. Go to **Schema Registry** → view the `KafkaMessage` schema
5. Go to **Consumer Groups** → check lag for all groups including `kafka-streams-demo`

---

### 4.10 Test 9 — Consumer Lag Check

```bash
# Inside broker container
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe --group kafka-streams-demo
```

```
GROUP              TOPIC          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
kafka-streams-demo my-topic       0          2               2               0
kafka-streams-demo my-topic       1          1               1               0
kafka-streams-demo my-topic       2          1               1               0
```

---

### 4.11 Quick Test Cheatsheet

```bash
# ── String messages ──────────────────────────────────────────────
curl -X POST "http://localhost:8085/api/kafka/send?message=hello world"
curl -X POST http://localhost:8085/api/kafka/send-keyed \
     -H "Content-Type: application/json" -d '{"key":"u1","message":"Hi there"}'

# ── Avro messages ────────────────────────────────────────────────
curl -X POST "http://localhost:8085/api/kafka/avro/send?message=HelloAvro"
curl -X POST http://localhost:8085/api/kafka/avro/send-keyed \
     -H "Content-Type: application/json" -d '{"key":"u1","message":"Hi Avro"}'

# ── Kafka Streams interactive queries ────────────────────────────
curl "http://localhost:8085/api/kafka/streams/wordcount?word=hello"
curl "http://localhost:8085/api/kafka/streams/wordcount/all"

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
    --bootstrap-server localhost:9092 --describe --group kafka-streams-demo

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

### 5.5 Kafka Streams: Production Tuning

```java
// KafkaStreamsConfig — production settings
props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 30_000);          // 30s commit interval
props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10485760L); // 10MB record cache
props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);               // 1 thread per partition
props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);               // match broker RF
props.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");
props.put(StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG), 10);
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
          StreamsConfig.EXACTLY_ONCE_V2);                            // EOS (requires RF≥3)
```

**State store — production backup:**

RocksDB state stores are automatically backed up to Kafka changelog topics. If the Streams
application crashes, it restores state from the changelog on restart — no data loss. In production:
- Keep the changelog topic's retention long enough to survive a full rebuild
- Use `standby.replicas` to maintain warm standbys for faster failover

```java
props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);  // 1 warm standby per partition
```

---

### 5.6 Topic Design

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

### 5.7 Security

```yaml
# docker-compose — enable SASL_SSL on broker
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: SASL_SSL:SASL_SSL,CONTROLLER:PLAINTEXT
KAFKA_LISTENERS: SASL_SSL://0.0.0.0:9092,CONTROLLER://broker:29093
KAFKA_SSL_KEYSTORE_LOCATION: /certs/kafka.keystore.jks
KAFKA_SASL_ENABLED_MECHANISMS: SCRAM-SHA-512
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

### 5.8 Monitoring & Alerting

**Key metrics to alert on:**

| Metric | Alert Threshold | What It Means |
|---|---|---|
| `kafka_consumer_lag` | > 10,000 | Consumer falling behind |
| `kafka_server_UnderReplicatedPartitions` | > 0 | Broker down or replication broken |
| `kafka_controller_ActiveControllerCount` | != 1 | No active controller |
| `kafka_streams_*_state` | not `RUNNING` | Streams topology crashed |
| `kafka_streams_*_commit_latency` | > 1000ms | State store flush too slow |

**App metrics** available at `http://localhost:8085/actuator/prometheus` (configured via `micrometer-registry-prometheus`).

---

### 5.9 Dead Letter Queue (DLQ) Pattern

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

### 5.10 Dev vs. Production Config Comparison

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
| Streams commit interval | 1s | 30s |
| Streams threads | 1 | match partition count |
| Streams EOS | off | `exactly_once_v2` |
| Streams standby replicas | 0 | 1+ |
| Security | `PLAINTEXT` | `SASL_SSL` |
| Control Center | trial (30 days) | licensed |
| Partitions | 3 | 12+ |
| Monitoring | Actuator + CC trial | Prometheus + Grafana + CC |
| DLQ | none | Required |

---

## Project Structure Reference

```
kafka-practical/
├── docker-compose.yaml
│   └── Confluent Platform 7.7: broker (KRaft), schema-registry, rest-proxy, control-center
├── src/
│   ├── main/
│   │   ├── avro/
│   │   │   └── KafkaMessage.avsc            # Avro schema → generates KafkaMessage.java
│   │   ├── java/com/example/kafka/
│   │   │   ├── KafkaApplication.java        # Spring Boot entry point
│   │   │   ├── config/
│   │   │   │   ├── KafkaConfig.java         # Topics + @Primary String beans + Avro beans
│   │   │   │   └── KafkaStreamsConfig.java  # @EnableKafkaStreams + KafkaStreamsConfiguration
│   │   │   ├── producer/
│   │   │   │   ├── MessageProducer.java     # String send() and sendWithKey()
│   │   │   │   └── AvroMessageProducer.java # Avro send() and sendWithKey()
│   │   │   ├── consumer/
│   │   │   │   ├── MessageConsumer.java     # String @KafkaListener (demo-group, demo-group-advanced)
│   │   │   │   └── AvroMessageConsumer.java # Avro @KafkaListener (demo-group-avro)
│   │   │   ├── streams/
│   │   │   │   └── MessageStreamsTopology.java  # KStream topology: uppercase + word count
│   │   │   ├── service/
│   │   │   │   └── WordCountQueryService.java  # Interactive queries against RocksDB state store
│   │   │   └── controller/
│   │   │       └── MessageController.java   # REST endpoints (see API table below)
│   │   └── resources/
│   │       └── application.yml             # Port 8085, streams config, topic names
│   └── test/
├── pom.xml                                  # Confluent repo, Avro, kafka-streams deps
├── mvnw / mvnw.cmd                          # Maven wrapper
├── KAFKA_DOCS.md                            # This file
├── CONFLUENT_PLATFORM.md                    # Schema Registry, REST Proxy, Control Center guide
└── .gitignore                               # Excludes target/, generated avro/, .idea/
```

### API Endpoints

| Method | URL | Description |
|---|---|---|
| `POST` | `/api/kafka/send?message=X` | Send String message (round-robin partition) |
| `POST` | `/api/kafka/send-keyed` | Send String message with key `{"key":"k","message":"v"}` |
| `POST` | `/api/kafka/avro/send?message=X` | Send Avro message (schema auto-registered) |
| `POST` | `/api/kafka/avro/send-keyed` | Send Avro message with key `{"key":"k","message":"v"}` |
| `GET`  | `/api/kafka/streams/wordcount?word=X` | Query word count from local state store |
| `GET`  | `/api/kafka/streams/wordcount/all` | Return all word counts from local state store |
| `GET`  | `http://localhost:8081/subjects` | List registered schemas (Schema Registry) |
| `GET`  | `http://localhost:9021` | Control Center web UI |
| `POST` | `http://localhost:8082/topics/{name}` | Produce via REST Proxy |
| `GET`  | `/actuator/health` | App health + Kafka Streams state |
| `GET`  | `/actuator/prometheus` | Prometheus metrics |
