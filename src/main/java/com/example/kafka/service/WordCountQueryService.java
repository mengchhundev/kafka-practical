package com.example.kafka.service;

import com.example.kafka.streams.MessageStreamsTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * Provides interactive queries against the Kafka Streams local state store.
 *
 * <p>Interactive queries let you read the current state of a stream processor
 * <em>without</em> publishing a query message to Kafka. The lookup hits the
 * in-process RocksDB store directly — sub-millisecond latency.
 *
 * <p>The store is only available once the Streams application reaches
 * {@link KafkaStreams.State#RUNNING}. Calls made before that throw
 * {@link IllegalStateException} with a descriptive message.
 */
@Service
public class WordCountQueryService {

    private static final Logger log = LoggerFactory.getLogger(WordCountQueryService.class);

    private final StreamsBuilderFactoryBean factoryBean;

    public WordCountQueryService(StreamsBuilderFactoryBean factoryBean) {
        this.factoryBean = factoryBean;
    }

    /**
     * Returns the current count for a single word.
     *
     * @param word the word to look up (case-insensitive)
     * @return current count, or {@code 0} if the word has never been seen
     */
    public long getCount(String word) {
        Long count = getStore().get(word.toLowerCase());
        return count != null ? count : 0L;
    }

    /**
     * Returns every word and its current count from the local state store.
     * Iterates the full RocksDB range scan — suitable for small state;
     * paginate in production.
     */
    public Map<String, Long> getAllCounts() {
        Map<String, Long> result = new HashMap<>();
        try (var iterator = getStore().all()) {
            iterator.forEachRemaining(kv -> result.put(kv.key, kv.value));
        }
        log.debug("[WordCountQuery] Scanned {} entries from state store", result.size());
        return result;
    }

    // ── Internal ─────────────────────────────────────────────────────────────

    private ReadOnlyKeyValueStore<String, Long> getStore() {
        KafkaStreams streams = factoryBean.getKafkaStreams();

        if (streams == null) {
            throw new IllegalStateException(
                    "Kafka Streams has not started yet (KafkaStreams is null)");
        }
        if (streams.state() != KafkaStreams.State.RUNNING) {
            throw new IllegalStateException(
                    "Kafka Streams is not RUNNING yet (current state: " + streams.state() + ")");
        }

        return streams.store(
                StoreQueryParameters.fromNameAndType(
                        MessageStreamsTopology.WORD_COUNT_STORE,
                        QueryableStoreTypes.keyValueStore()));
    }
}
