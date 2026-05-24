package com.example.kafka.controller;

import com.example.kafka.producer.AvroMessageProducer;
import com.example.kafka.producer.MessageProducer;
import com.example.kafka.service.WordCountQueryService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/kafka")
public class MessageController {

    private final MessageProducer producer;
    private final AvroMessageProducer avroProducer;
    private final WordCountQueryService wordCountQueryService;

    public MessageController(MessageProducer producer,
                             AvroMessageProducer avroProducer,
                             WordCountQueryService wordCountQueryService) {
        this.producer = producer;
        this.avroProducer = avroProducer;
        this.wordCountQueryService = wordCountQueryService;
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

    // ── Kafka Streams — interactive queries ──────────────────────────────────

    /**
     * Query the current count of a single word from the local state store.
     *
     * <p>Example: GET /api/kafka/streams/wordcount?word=hello
     * <p>Returns 503 if Kafka Streams has not reached RUNNING state yet.
     */
    @GetMapping("/streams/wordcount")
    public ResponseEntity<?> getWordCount(@RequestParam String word) {
        try {
            long count = wordCountQueryService.getCount(word);
            return ResponseEntity.ok(Map.of("word", word, "count", count));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(503).body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Returns all words and their counts from the local state store.
     *
     * <p>Example: GET /api/kafka/streams/wordcount/all
     * <p>Returns 503 if Kafka Streams has not reached RUNNING state yet.
     */
    @GetMapping("/streams/wordcount/all")
    public ResponseEntity<?> getAllWordCounts() {
        try {
            return ResponseEntity.ok(wordCountQueryService.getAllCounts());
        } catch (IllegalStateException e) {
            return ResponseEntity.status(503).body(Map.of("error", e.getMessage()));
        }
    }
}
