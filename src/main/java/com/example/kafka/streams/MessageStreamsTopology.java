package com.example.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;

/**
 * Defines the Kafka Streams processing topology.
 *
 * <p>Source topic: {@code my-topic} (plain String messages)
 *
 * <pre>
 *   my-topic
 *     ├── mapValues(toUpperCase) ─────────────────────────────► my-topic-uppercase
 *     └── flatMapValues(split words)
 *           → filter(non-empty)
 *           → selectKey(word)        ← repartition by word
 *           → groupByKey
 *           → count(state-store)
 *           → toStream ─────────────────────────────────────► my-topic-wordcount
 * </pre>
 *
 * <p>The word-count state store ({@link #WORD_COUNT_STORE}) is queryable via
 * {@link com.example.kafka.service.WordCountQueryService} without hitting Kafka.
 */
@Configuration
public class MessageStreamsTopology {

    private static final Logger log = LoggerFactory.getLogger(MessageStreamsTopology.class);

    /** Name of the RocksDB-backed state store exposed for interactive queries. */
    public static final String WORD_COUNT_STORE = "word-count-store";

    @Value("${app.kafka.topic}")
    private String inputTopic;

    @Value("${app.kafka.uppercase-topic}")
    private String uppercaseTopic;

    @Value("${app.kafka.wordcount-topic}")
    private String wordcountTopic;

    /**
     * Registers both processing branches on the shared {@link StreamsBuilder}.
     * Spring Kafka calls this method before starting the Streams application so
     * both branches become part of the same compiled topology.
     *
     * @param builder injected by {@code @EnableKafkaStreams} infrastructure
     * @return the source stream (not used downstream; returned for Spring to manage)
     */
    @Bean
    public KStream<String, String> messageStream(StreamsBuilder builder) {

        KStream<String, String> source = builder.stream(inputTopic);

        // ── Branch 1: Stateless uppercase transformation ─────────────────────
        //   Input:  (key, "hello world")
        //   Output: (key, "HELLO WORLD")  → my-topic-uppercase
        source
                .mapValues(value -> {
                    log.debug("[Streams][Uppercase] '{}' → '{}'", value, value.toUpperCase());
                    return value.toUpperCase();
                })
                .to(uppercaseTopic);

        // ── Branch 2: Stateful word count ────────────────────────────────────
        //   Input:  (key, "hello world hello")
        //   Output: ("hello", 2L), ("world", 1L) → my-topic-wordcount
        source
                // Split each message value into individual words
                .flatMapValues(value ->
                        Arrays.asList(value.toLowerCase().split("\\W+")))
                // Drop empty strings produced by leading/trailing punctuation
                .filter((key, word) -> word != null && !word.isEmpty())
                // Re-key by the word itself so groupByKey aggregates correctly.
                // NOTE: selectKey causes an internal repartition topic to be created
                //       automatically by Kafka Streams.
                .selectKey((key, word) -> word)
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                // Materialize counts into a named, queryable RocksDB state store
                .count(Materialized
                        .<String, Long, KeyValueStore<Bytes, byte[]>>as(WORD_COUNT_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))
                .toStream()
                .peek((word, count) ->
                        log.info("[Streams][WordCount] '{}' = {}", word, count))
                .to(wordcountTopic, Produced.with(Serdes.String(), Serdes.Long()));

        return source;
    }
}
