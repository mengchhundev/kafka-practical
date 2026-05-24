package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class MessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(MessageConsumer.class);

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "demo-group")
    public void listen(String message) {
        log.info("Received message: {}", message);
    }

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "demo-group-advanced")
    public void listenWithMetadata(ConsumerRecord<String, String> record) {
        log.info("Received record -> topic={}, partition={}, offset={}, key={}, value={}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value());
    }
}
