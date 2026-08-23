package io.github.kafkademo.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.kafkademo.domain.DltMessage;
import io.github.kafkademo.domain.Message;
import io.github.kafkademo.domain.MessagePayload;
import io.github.kafkademo.feed.MessageFeed;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

/**
 * The consumer role, now with the full error-handling stack (docs/07):
 *
 * - {@link RetryableTopic}: on a listener exception the record moves through
 *   "messages-retry-1000" → "messages-retry-2000" → "messages-retry-4000"
 *   (increasing backoff), then to the dead-letter topic "messages-dlt".
 * - {@link DltHandler}: dead-lettered records are stored and pushed to the UI live.
 * - Dedup: a {@link MessagePayload} id seen before is skipped (idempotent consumption).
 *
 * A message containing "boom" is the poison trigger for the demo — it always throws.
 * JSON that can't be parsed is excluded from retries and goes straight to the DLT
 * (retrying a permanent parse failure just burns cycles).
 */
@Component
@Slf4j
public class MessageConsumer {

    private final MessageStore store;
    private final DltStore dltStore;
    private final DedupStore dedup;
    private final MessageFeed feed;
    private final ObjectMapper objectMapper;

    public MessageConsumer(MessageStore store, DltStore dltStore, DedupStore dedup,
                           MessageFeed feed, ObjectMapper objectMapper) {
        this.store = store;
        this.dltStore = dltStore;
        this.dedup = dedup;
        this.feed = feed;
        this.objectMapper = objectMapper;
    }

    @RetryableTopic(
            attempts = "4",                       // original + 3 retries
            numPartitions = "3",
            backoff = @Backoff(delay = 1000, multiplier = 2.0),
            exclude = { com.fasterxml.jackson.core.JsonProcessingException.class })
    @KafkaListener(topics = "${spring.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(ConsumerRecord<Integer, String> record) throws com.fasterxml.jackson.core.JsonProcessingException {
        MessagePayload payload = objectMapper.readValue(record.value(), MessagePayload.class);

        if (dedup.isProcessed(payload.id())) {
            log.info("Duplicate skipped id={} (key={} partition={} offset={})",
                    payload.id(), record.key(), record.partition(), record.offset());
            return;
        }

        if (payload.content().contains("boom")) {
            throw new RuntimeException("poison message: " + payload.content());
        }

        Message message = new Message(payload.id(), record.partition(), record.offset(),
                payload.content(), record.timestamp());
        store.add(message);
        feed.broadcast(message);
        log.info("Consumed id={} key={} -> partition={} offset={} value='{}'",
                payload.id(), record.key(), record.partition(), record.offset(), payload.content());

        // Mark only after success so a failed attempt is never considered processed.
        dedup.markProcessed(payload.id());
    }

    @DltHandler
    public void onDlt(ConsumerRecord<Integer, String> record,
                      @Header(KafkaHeaders.EXCEPTION_FQCN) String errorClass,
                      @Header(name = KafkaHeaders.EXCEPTION_CAUSE_FQCN, required = false) String causeFqcn,
                      @Header(KafkaHeaders.EXCEPTION_STACKTRACE) String stackTrace,
                      @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        String content = record.value();
        String id = "unknown";
        try {
            MessagePayload payload = objectMapper.readValue(content, MessagePayload.class);
            content = payload.content();
            id = payload.id();
        } catch (Exception ignored) {
            // best-effort: keep the raw string when the DLT record isn't valid JSON
        }
        // The top-level exception is usually the framework wrapper; prefer the root cause.
        String error = (causeFqcn != null && !causeFqcn.isBlank()) ? causeFqcn : errorClass;
        DltMessage dlt = new DltMessage(id, record.partition(), record.offset(), content, error, record.timestamp());
        dltStore.add(dlt);
        feed.broadcastDlt(dlt);
        log.error("Dead-lettered id={} partition={} offset={} error={} on topic '{}'",
                id, record.partition(), record.offset(), error, topic);
    }
}
