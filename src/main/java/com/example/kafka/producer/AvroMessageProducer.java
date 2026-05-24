package com.example.kafka.producer;

import com.example.kafka.avro.KafkaMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class AvroMessageProducer {

    private static final Logger log = LoggerFactory.getLogger(AvroMessageProducer.class);

    private final KafkaTemplate<String, Object> avroKafkaTemplate;

    @Value("${app.kafka.avro-topic}")
    private String avroTopic;

    public AvroMessageProducer(@Qualifier("avroKafkaTemplate") KafkaTemplate<String, Object> avroKafkaTemplate) {
        this.avroKafkaTemplate = avroKafkaTemplate;
    }

    /**
     * Sends an Avro-serialized message. Schema is registered automatically
     * in Confluent Schema Registry on first publish.
     */
    public void send(String content) {
        KafkaMessage message = KafkaMessage.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setKey(null)
                .setContent(content)
                .setTimestamp(Instant.now().toEpochMilli())
                .build();

        CompletableFuture<SendResult<String, Object>> future =
                avroKafkaTemplate.send(avroTopic, message.getId(), message);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("[Avro] Sent id={} content='{}' -> partition={}, offset={}",
                        message.getId(),
                        content,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("[Avro] Failed to send content='{}': {}", content, ex.getMessage());
            }
        });
    }

    /**
     * Sends an Avro-serialized message with an explicit routing key.
     * Same key always routes to the same partition.
     */
    public void sendWithKey(String key, String content) {
        KafkaMessage message = KafkaMessage.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setKey(key)
                .setContent(content)
                .setTimestamp(Instant.now().toEpochMilli())
                .build();

        CompletableFuture<SendResult<String, Object>> future =
                avroKafkaTemplate.send(avroTopic, key, message);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("[Avro] Sent key='{}' content='{}' -> partition={}, offset={}",
                        key, content,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("[Avro] Failed to send key='{}' content='{}': {}", key, content, ex.getMessage());
            }
        });
    }
}
