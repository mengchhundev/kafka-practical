package com.example.kafka.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * Enables and configures Kafka Streams within Spring Boot.
 *
 * <p>{@code @EnableKafkaStreams} registers a {@link org.springframework.kafka.config.StreamsBuilderFactoryBean}
 * and a {@link org.apache.kafka.streams.StreamsBuilder} in the application context.
 * Any {@code @Bean} method that accepts a {@code StreamsBuilder} parameter will
 * contribute nodes to the shared topology before the Streams application starts.
 */
@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    /**
     * The bean name {@link KafkaStreamsDefaultConfiguration#DEFAULT_STREAMS_CONFIG_BEAN_NAME}
     * is the magic name Spring Kafka looks for to override default Streams settings.
     */
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfiguration() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,      "kafka-streams-demo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,   bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,   Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put("schema.registry.url",                    schemaRegistryUrl);

        // Commit state-store changes every second (lower latency for dev; tune in prod)
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        // Single broker — set replication factor to 1 for internal changelog topics
        props.put("replication.factor", 1);

        return new KafkaStreamsConfiguration(props);
    }
}
