package com.example.kafka.controller;

import com.example.kafka.producer.AvroMessageProducer;
import com.example.kafka.producer.MessageProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/kafka")
public class MessageController {

    private final MessageProducer producer;
    private final AvroMessageProducer avroProducer;

    public MessageController(MessageProducer producer, AvroMessageProducer avroProducer) {
        this.producer = producer;
        this.avroProducer = avroProducer;
    }

    // ── String messages ──────────────────────────────────────────────────────

    @PostMapping("/send")
    public ResponseEntity<String> send(@RequestParam String message) {
        producer.send(message);
        return ResponseEntity.ok("Message sent: " + message);
    }

    @PostMapping("/send-keyed")
    public ResponseEntity<String> sendKeyed(@RequestBody Map<String, String> body) {
        String key = body.get("key");
        String message = body.get("message");
        producer.sendWithKey(key, message);
        return ResponseEntity.ok("Keyed message sent: key=" + key + ", message=" + message);
    }

    // ── Avro messages (Schema Registry) ─────────────────────────────────────

    @PostMapping("/avro/send")
    public ResponseEntity<String> sendAvro(@RequestParam String message) {
        avroProducer.send(message);
        return ResponseEntity.ok("Avro message sent: " + message);
    }

    @PostMapping("/avro/send-keyed")
    public ResponseEntity<String> sendAvroKeyed(@RequestBody Map<String, String> body) {
        String key = body.get("key");
        String message = body.get("message");
        avroProducer.sendWithKey(key, message);
        return ResponseEntity.ok("Avro keyed message sent: key=" + key + ", message=" + message);
    }
}
