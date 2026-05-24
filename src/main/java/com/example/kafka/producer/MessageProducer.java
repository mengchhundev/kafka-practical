package com.example.kafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class MessageProducer {

    private static final Logger log = LoggerFactory.getLogger(MessageProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${app.kafka.topic}")
    private String topic;

    public MessageProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String message) {
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, message);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Sent [{}] -> partition={}, offset={}",
                        message,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Failed to send [{}]: {}", message, ex.getMessage());
            }
        });
    }

    public void sendWithKey(String key, String message) {
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, message);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Sent key=[{}] value=[{}] -> partition={}, offset={}",
                        key,
                        message,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Failed to send key=[{}] value=[{}]: {}", key, message, ex.getMessage());
            }
        });
    }
}
