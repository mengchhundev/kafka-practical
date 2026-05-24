package com.example.kafka.consumer;

import com.example.kafka.avro.KafkaMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class AvroMessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(AvroMessageConsumer.class);

    /**
     * Consumes Avro messages deserialized via Confluent Schema Registry.
     * The schema is fetched automatically from Schema Registry using the
     * schema ID embedded in the message bytes (magic byte + schema ID prefix).
     */
    @KafkaListener(
            topics = "${app.kafka.avro-topic}",
            groupId = "demo-group-avro",
            containerFactory = "avroKafkaListenerContainerFactory"
    )
    public void listen(ConsumerRecord<String, KafkaMessage> record) {
        KafkaMessage msg = record.value();

        log.info("[Avro] Received → topic={}, partition={}, offset={} | " +
                "id={}, key={}, content='{}', sentAt={}",
                record.topic(),
                record.partition(),
                record.offset(),
                msg.getId(),
                msg.getKey(),
                msg.getContent(),
                Instant.ofEpochMilli(msg.getTimestamp()));
    }
}
