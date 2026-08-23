package io.github.kafkademo.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.kafkademo.domain.MessagePayload;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The producer role. Each message is a {@link MessagePayload} serialized to JSON, sent as
 * a {@link ProducerRecord} with a schema-version header (see docs/06). An incrementing
 * integer key spreads records across partitions via murmur2(key) % partitions (docs/03).
 */
@Service
@Slf4j
public class MessageProducer {

    private final KafkaTemplate<Integer, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final AtomicInteger keyCounter = new AtomicInteger();

    @Value("${spring.kafka.topic}")
    private String topic;

    public MessageProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Fire the async send; success/failure is handled in the {@code whenComplete} callback
     * (the future is what the REST controller blocks on to show partition/offset).
     */
    public CompletableFuture<SendResult<Integer, String>> send(String content) throws JsonProcessingException {
        MessagePayload payload = new MessagePayload(content);
        String json = objectMapper.writeValueAsString(payload);

        ProducerRecord<Integer, String> record = new ProducerRecord<>(
                topic, null, nextKey(), json,
                List.of(new RecordHeader("schema-version", "1".getBytes())));

        log.info("Producing key={} id={} value='{}' to topic '{}'", record.key(), payload.id(), content, topic);
        return kafkaTemplate.send(record)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send key={} id={}", record.key(), payload.id(), ex);
                    } else {
                        RecordMetadata meta = result.getRecordMetadata();
                        log.info("Sent key={} id={} -> topic={} partition={} offset={}",
                                record.key(), payload.id(), meta.topic(), meta.partition(), meta.offset());
                    }
                });
    }

    private Integer nextKey() {
        return keyCounter.incrementAndGet();
    }
}
