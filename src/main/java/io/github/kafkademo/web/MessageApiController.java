package io.github.kafkademo.web;

import io.github.kafkademo.domain.MessageRequest;
import io.github.kafkademo.feed.MessageFeed;
import io.github.kafkademo.producer.MessageProducer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * REST API: POST /api/messages (produce), GET /api/messages/stream (SSE live feed).
 * Blocking the web thread with .get(5s) is deliberate here — the learning payoff is the
 * partition/offset in the response (see docs/03 §3 for the async alternative).
 */
@RestController
public class MessageApiController {

    private final MessageProducer producer;
    private final MessageFeed feed;

    public MessageApiController(MessageProducer producer, MessageFeed feed) {
        this.producer = producer;
        this.feed = feed;
    }

    @PostMapping("/api/messages")
    public ResponseEntity<?> postMessage(@RequestBody MessageRequest request) {
        if (request.content() == null || request.content().isBlank()) {
            return ResponseEntity.badRequest().body("content must not be blank");
        }
        try {
            SendResult<Integer, String> result = producer.send(request.content()).get(5, TimeUnit.SECONDS);
            return ResponseEntity.ok(Map.of(
                    "key", result.getProducerRecord().key(),
                    "partition", result.getRecordMetadata().partition(),
                    "offset", result.getRecordMetadata().offset()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Kafka send failed: " + e.getMessage());
        }
    }

    @GetMapping(value = "/api/messages/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        return feed.subscribe();
    }
}
