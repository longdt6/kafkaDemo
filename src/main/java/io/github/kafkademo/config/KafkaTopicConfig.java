package io.github.kafkademo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the "messages" topic so the auto-configured KafkaAdmin creates it at startup
 * with the right partitions/replicas — instead of the broker's default auto-create
 * (1 partition, 1 replica). See docs/05 and docs/08.
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic messagesTopic(@Value("${app.topic.replicas:3}") int replicas) {
        return TopicBuilder.name("messages")
                .partitions(3)
                .replicas(replicas)
                .build();
    }
}
