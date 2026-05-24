package com.example.kafka.controller;

import com.example.kafka.producer.MessageProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/kafka")
public class MessageController {

    private final MessageProducer producer;

    public MessageController(MessageProducer producer) {
        this.producer = producer;
    }

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
}
