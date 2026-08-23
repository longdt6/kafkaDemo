# 05 — Spring Kafka Configuration: `application.yml` → Client Configs

> Spring Boot's auto-configuration is the whole game: it reads `spring.kafka.*` properties and builds the `KafkaProducer`, `KafkaConsumer`, `KafkaAdmin`, and `KafkaTemplate` beans for you. This doc maps every key in this project's YAML to the underlying Kafka client config, so a config you read in the Kafka docs (`linger.ms`, `fetch.min.bytes`, …) becomes a property you can set.

---

## 1. The one-key-that-does-three-jobs rule: `spring.kafka.bootstrap-servers`

Spring Boot splits Kafka settings into `producer`, `consumer`, `admin`, `template`, `listener`, and a **top-level** `spring.kafka.*`. The key insight: **top-level values are inherited by all the sub-objects unless overridden.**

```yaml
spring:
  kafka:
    bootstrap-servers: kafka-1:19092,kafka-2:19093,kafka-3:19094   # ← set ONCE
    producer:
      key-serializer: ...
      value-serializer: ...
    consumer:
      key-deserializer: ...
      value-deserializer: ...
      group-id: kafka-demo-group
    admin:
      fail-fast: true
```

`spring.kafka.bootstrap-servers` propagates to the **producer, consumer, admin, and template** at once (it's copied into each `commonProperties`). You only need per-section `bootstrap-servers` when one role must talk to a *different* cluster (e.g. a consumer reading from cluster A and a producer writing to cluster B).

---

## 2. Property mapping table

Every `spring.kafka.<role>.*` key maps to a Kafka client config with the same name (dashes → dots). The `properties` sub-tree lets you set **any** client config Spring doesn't name explicitly.

| `application.yml` | Kafka client config | Where it lands |
|---|---|---|
| `spring.kafka.bootstrap-servers` | `bootstrap.servers` | producer, consumer, admin, template |
| `spring.kafka.producer.key-serializer` | `key.serializer` | producer |
| `spring.kafka.producer.value-serializer` | `value.serializer` | producer |
| `spring.kafka.producer.properties.acks` | `acks` | producer |
| `spring.kafka.producer.properties.retries` | `retries` | producer |
| `spring.kafka.producer.properties.retry.backoff.ms` | `retry.backoff.ms` | producer |
| `spring.kafka.producer.properties.enable.idempotence` | `enable.idempotence` | producer |
| `spring.kafka.producer.properties.linger.ms` | `linger.ms` | producer |
| `spring.kafka.consumer.key-deserializer` | `key.deserializer` | consumer |
| `spring.kafka.consumer.value-deserializer` | `value.deserializer` | consumer |
| `spring.kafka.consumer.group-id` | `group.id` | consumer |
| `spring.kafka.consumer.auto-offset-reset` | `auto.offset.reset` | consumer |
| `spring.kafka.consumer.enable-auto-commit` | `enable.auto.commit` | consumer |
| `spring.kafka.consumer.properties.*` | *any consumer config* | consumer |
| `spring.kafka.template.default-topic` | default topic for `sendDefault()` | template |
| `spring.kafka.admin.fail-fast` | — (Spring behavior) | admin |
| `spring.kafka.listener.concurrency` | — (Spring: # of containers) | listener |

Rule of thumb: **if it exists in the Kafka docs as a producer/consumer config and Spring doesn't have a dedicated YAML key, put it under `properties`.**

---

## 3. Custom properties: `spring.kafka.topic`

`spring.kafka.topic` is **not** a real Spring Boot property — it's a *custom* key we invented to hold the topic name (same convention as the course's `application.yml`). You read it anywhere with `@Value`:

```java
@Value("${spring.kafka.topic}")
private String topic;
```

and reference it in annotations too:

```java
@KafkaListener(topics = "${spring.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
```

This keeps the topic name in exactly one place (the config) — change it once, and both producer and consumer move to the new topic. (Real systems often use `@ConfigurationProperties` for this instead of `@Value`; both are worth knowing, `@Value` is what the course uses.)

---

## 4. Profiles: `default` vs `local`

Two profiles, one switchable property each (`bootstrap-servers`):

```yaml
# application.yml — DEFAULT profile (used inside docker)
spring.kafka.bootstrap-servers: kafka-1:19092,kafka-2:19093,kafka-3:19094   # INTERNAL listeners

# application-local.yml — LOCAL profile (run on your Mac, brokers only)
spring.kafka.bootstrap-servers: localhost:9092,localhost:9093,localhost:9094  # EXTERNAL listeners
```

- Spring loads `application.yml` always; a profile-specific file **overrides only the keys it contains**. So `application-local.yml` with a single `bootstrap-servers` key is all it takes to switch clusters.
- Activate: `SPRING_PROFILES_ACTIVE=local` (or `--spring.profiles.active=local`).
- **The demo image always runs `default`** (see `02`) so it talks INTERNAL listeners. Running `local` inside the container would make `localhost` point at the container itself and nothing would connect.

This two-profile setup is the same idea as the course's multi-document `on-profile:` YAML blocks, but with one key per file instead of duplicated blocks.

---

## 5. The beans Spring Boot auto-configures

Because `spring-kafka` is on the classpath, Spring Boot creates (all overridable):

| Bean | Built from | Used by |
|---|---|---|
| `KafkaTemplate<Object,Object>` (as `KafkaTemplate<Integer,String>` via generics) | `spring.kafka.producer.*` | `MessageProducer` |
| `KafkaAdmin` | `spring.kafka.admin.*` | creates `NewTopic` beans at startup |
| `ConcurrentKafkaListenerContainerFactory` | `spring.kafka.listener.*` + `consumer.*` | powers every `@KafkaListener` |
| `ConsumerFactory` | `spring.kafka.consumer.*` | the container factory above |
| `KafkaConsumer`/`KafkaProducer` (when you opt out of auto-config) | manual config | low-level clients |

`KafkaAdmin` is what makes the `NewTopic` bean in `KafkaTopicConfig` take effect — at startup it ensures the topic exists with the requested partitions/replicas:

```java
@Bean
public NewTopic messagesTopic(@Value("${app.topic.replicas:3}") int replicas) {
    return TopicBuilder.name("messages").partitions(3).replicas(replicas).build();
}
```

Note `app.topic.replicas` — a custom property we override to `1` in tests (`src/test/resources/application.yml`) because the embedded broker is a single node.

---

## 6. Official references

- [Spring Boot — Messaging with Kafka (properties reference)](https://docs.spring.io/spring-boot/reference/messaging/kafka.html)
- [Spring for Apache Kafka — Reference (templates, listeners, containers)](https://docs.spring.io/spring-kafka/reference/)
- [Kafka producer configs](https://kafka.apache.org/documentation/#producerconfigs) / [consumer configs](https://kafka.apache.org/documentation/#consumerconfigs) — the canonical knob names

---

## 7. Hands-on in this project

```bash
# Run on the Mac WITHOUT dockerizing the app (brokers must be up: docker compose up -d kafka-1 kafka-2 kafka-3)
./mvnw spring-boot:run -Dspring-boot.run.profiles=local

# Same app, different cluster. Prove it by changing application-local.yml to a bogus port →
# app fails to connect at startup (admin.fail-fast), which itself proves the config flows through.
```

**Experiment:** add `spring.kafka.producer.properties.linger.ms: 10` and `batch.size: 16384` to `application.yml`, rebuild, and watch throughput/delay change. Tuning the producer is now a YAML edit, not a code edit — that's the payoff of §2.

---

**Next:** [06 — Serialization: from bytes to objects and back](06-serialization.md)
