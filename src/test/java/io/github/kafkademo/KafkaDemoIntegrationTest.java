package io.github.kafkademo;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.kafkademo.consumer.DedupStore;
import io.github.kafkademo.consumer.DltStore;
import io.github.kafkademo.consumer.MessageStore;
import io.github.kafkademo.domain.MessagePayload;
import io.github.kafkademo.producer.MessageProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end round trips against an in-process Kafka broker (no Docker needed).
 * Covers the Phase 3 patterns: JSON round trip, poison→retry→DLT, idempotent dedup.
 *
 * The one property override points ALL of Spring Boot's Kafka auto-config
 * (producer, consumer, template, admin) at the embedded broker. See docs/05.
 */
@SpringBootTest
@EmbeddedKafka(topics = "messages", partitions = 3)
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class KafkaDemoIntegrationTest {

    @Autowired
    MessageProducer messageProducer;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    MessageStore messageStore;

    @Autowired
    DltStore dltStore;

    @Autowired
    DedupStore dedupStore;

    @Test
    void producedMessageIsConsumedIntoStore() throws Exception {
        messageProducer.send("hello kafka").get(5, TimeUnit.SECONDS);

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertThat(messageStore.getMessages()).hasSize(1);
                    assertThat(messageStore.getMessages().get(0).content()).isEqualTo("hello kafka");
                    assertThat(messageStore.getMessages().get(0).offset()).isEqualTo(0);
                });
    }

    @Test
    void poisonMessageRetriesThenLandsInDlt() throws Exception {
        messageProducer.send("boom please fail").get(5, TimeUnit.SECONDS);

        await().atMost(15, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertThat(dltStore.getMessages()).hasSize(1);
                    assertThat(dltStore.getMessages().get(0).content()).isEqualTo("boom please fail");
                    // RuntimeException (wrapped) is what finally exhausted the retries
                    assertThat(dltStore.getMessages().get(0).errorClass()).contains("RuntimeException");
                });

        // The poison record must never have reached the visible store.
        assertThat(messageStore.getMessages()).isEmpty();
    }

    @Test
    void duplicateDeliveryIsSkipped() throws Exception {
        // Same payload id sent twice = the same logical message delivered twice.
        String json = objectMapper.writeValueAsString(new MessagePayload("dup-test"));
        kafkaTemplate.send("messages", 1, json).get(5, TimeUnit.SECONDS);
        kafkaTemplate.send("messages", 1, json).get(5, TimeUnit.SECONDS);

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(messageStore.getMessages()).hasSize(1));
        // give the (skipped) second delivery a moment — it must not have added anything
        Thread.sleep(1500);
        assertThat(messageStore.getMessages()).hasSize(1);
    }
}
