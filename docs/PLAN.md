# PLAN — Implementation Plan (Phases 2 & 3)

> **Status:** Phase 1 (docs 01–09) done · Phase 2 (the app + stack) done & verified live · Phase 3 (JSON + idempotency + retry/DLT) done & tested. This file describes *what we're building*; the learning docs in this folder describe *how Kafka works*.

## Context

Greenfield learning project in `/Users/dolong/Documents/kafkaDemo/`. Goal: demo/config how a Java 21 Spring Boot app acts as a Kafka client — a Thymeleaf page to type a message → REST → producer → 3-node Kafka cluster → `@KafkaListener` consumer → message pushed live to the page. Deployed locally via docker-compose using one auto build+deploy script.

**Confirmed decisions:** Thymeleaf UI · one app (producer + consumer roles) · 3-node KRaft cluster (no ZooKeeper) · SSE live-update display · include tests (@EmbeddedKafka).

## Verified stack (as of Aug 2026)

| Component | Version | Why |
|---|---|---|
| Java | 21 | Boot 3.5.x supports it |
| Spring Boot parent | 3.5.16 | current 3.x patch; don't chase 4.x (course patterns differ) |
| kafka-clients / spring-kafka | 3.9.2 / 3.3.16 | managed by Boot's BOM — matches broker exactly |
| Broker image | `apache/kafka:3.9.2` | pin; client = broker version = zero skew |
| Build base | `maven:3.9.16-eclipse-temurin-21` | |
| Runtime base | `eclipse-temurin:21-jre` | matches miniKafka convention |

**Critical image facts (verified from `KafkaDockerWrapper.scala`):** the official image **requires `CLUSTER_ID`** (same on all nodes) and converts every `KAFKA_*` env var to a dotted `kafka.*` config key. Runs as `appuser`; default `log.dirs` = `/tmp/kraft-combined-logs`. Old cp-kafka/ZooKeeper vars (`KAFKA_ZOOKEEPER_CONNECT`, `KAFKA_BROKER_ID`) must NOT be used.

## Project layout

```
kafkaDemo/
├── pom.xml  .gitignore  .dockerignore  Dockerfile  build-deploy.sh  docker-compose.yml
├── mvnw / mvnw.cmd / .mvn/wrapper/…        # Maven wrapper (reproducible build)
├── src/main/java/io/github/kafkademo/
│   ├── KafkaDemoApp.java
│   ├── config/KafkaTopicConfig.java        # NewTopic "messages" (3 partitions, 3 replicas)
│   ├── domain/Message.java                 # record(id, partition, offset, content, timestamp)
│   ├── domain/MessageRequest.java          # record(content)
│   ├── producer/MessageProducer.java       # KafkaTemplate<Integer,String> + whenComplete
│   ├── consumer/MessageStore.java          # CopyOnWriteArrayList, cap 100, snapshot copy
│   ├── consumer/MessageConsumer.java       # @KafkaListener
│   ├── feed/MessageFeed.java               # SseEmitter broadcast
│   └── web/MessageApiController.java       # POST /api/messages, GET /api/messages/stream (SSE)
│   └── web/MessageViewController.java      # GET / → Thymeleaf
├── src/main/resources/
│   ├── application.yml                     # default profile = inside docker (INTERNAL listeners)
│   ├── application-local.yml               # host run → localhost:9092,9093,9094
│   ├── templates/index.html                # form + SSE list
│   └── static/style.css                    # adapt from miniKafka
├── src/test/java/io/github/kafkademo/KafkaDemoIntegrationTest.java   # @EmbeddedKafka
├── src/test/resources/application.yml      # app.topic.replicas: 1
└── docs/                                   # ← you are here; learning material 01–09
```

Package convention mirrors miniKafka (`io.github.<project>`). Use Lombok `@Slf4j` (matches the course the user studies).

## docker-compose.yml — 3-node KRaft (the fiddly part)

`name: kafka-demo`. The app is a 4th service, connects via INTERNAL listener hostnames. All 3 nodes share the same `CLUSTER_ID` and `KAFKA_CONTROLLER_QUORUM_VOTERS`. Controller ports are 9093/9094/9095 per node (never collide with host-mapped EXTERNAL 9092/9093/9094).

Per node (kafka-1 shown; kafka-2/3 differ only in node id, ports, hostname, listeners):

```yaml
kafka-1:
  image: apache/kafka:3.9.2
  ports: ["9092:9092"]
  environment:
    KAFKA_NODE_ID: 1
    KAFKA_PROCESS_ROLES: broker,controller
    KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:9093,2@kafka-2:9094,3@kafka-3:9095
    KAFKA_LISTENERS: INTERNAL://0.0.0.0:19092,CONTROLLER://0.0.0.0:9093,EXTERNAL://0.0.0.0:9092
    KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka-1:19092,EXTERNAL://localhost:9092
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT
    KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
    KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
    CLUSTER_ID: 5L6g3nShT-eMCtK--X86sw
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
    KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
  healthcheck:
    test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:19092 >/dev/null 2>&1"]
    interval: 10s   timeout: 10s   retries: 30   start_period: 20s
  volumes: [kafka-1-data:/tmp/kraft-combined-logs]
```

`app` service: `build: .`, port 8080, `SPRING_PROFILES_ACTIVE: default`, `depends_on` all 3 brokers with `condition: service_healthy`, `restart: on-failure`. Named volumes `kafka-1-data/2-data/3-data`.

**Validate brokers alone first** (`docker compose up -d kafka-1 kafka-2 kafka-3` → `docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list`) before writing app code.

## application.yml

Top-level `spring.kafka.bootstrap-servers` is inherited by producer/consumer/template/admin — set once per profile.

- **application.yml (default/docker):** `bootstrap-servers: kafka-1:19092,kafka-2:19093,kafka-3:19094`; custom `spring.kafka.topic: messages`; `template.default-topic: messages`; producer `IntegerSerializer`/`StringSerializer`, `acks: all`, `retries: 10`, `retry.backoff.ms: 1000`, `enable.idempotence: true`; consumer `IntegerDeserializer`/`StringDeserializer`, `group-id: kafka-demo-group`, `auto-offset-reset: latest`; admin `fail-fast: true`.
- **application-local.yml:** only overrides `bootstrap-servers: localhost:9092,localhost:9093,localhost:9094` (host run via `./mvnw spring-boot:run -Dspring-boot.run.profiles=local`). Never use `local` inside docker — localhost there is the container itself.

## Java classes (patterns from the course + miniKafka)

- **MessageProducer**: `kafkaTemplate.send(topic, key, content)` with `AtomicInteger` key (integer key → murmur2 → partitions cycle), `.whenComplete` logging success (topic/partition/offset) / failure.
- **MessageConsumer**: `@KafkaListener(topics = "${spring.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")`, converts `ConsumerRecord` → `Message`, adds to `MessageStore`, `feed.broadcast`.
- **MessageFeed**: `CopyOnWriteArrayList<SseEmitter>`, `subscribe()` (60s timeout, cleanup on completion/timeout), `broadcast` sends `SseEmitter.event().name("message")`.
- **MessageApiController**: POST returns `Map.of("key",…,"partition",…,"offset",…)` (blocks ~5s on the future so the response shows partition/offset — the learning payoff; document async alternative). GET `/api/messages/stream` returns `SseEmitter` (`produces = TEXT_EVENT_STREAM_VALUE`).
- **KafkaTopicConfig**: `TopicBuilder.name("messages").partitions(3).replicas(@Value("${app.topic.replicas:3}"))` — override to 1 in tests (embedded broker is single-node).

## UI (index.html)

Form (fetch POST to `/api/messages`, alert on non-OK) + table (partition/offset/content/timestamp, server-rendered on load from `MessageStore`) + `<script>` with `EventSource('/api/messages/stream')` appending rows via `prepend` (newest on top). Style from miniKafka.

## Dockerfile + build-deploy.sh

- **Dockerfile** (multi-stage): `FROM maven:3.9.16-eclipse-temurin-21 AS build` → `COPY . .` → `mvn -q -DskipTests package` → `FROM eclipse-temurin:21-jre` → copy jar → `ENTRYPOINT ["java","-jar","app.jar"]`.
- **.dockerignore**: `target/ .git/ .idea/ *.iml .DS_Store docs/`.
- **build-deploy.sh** (`set -euo pipefail`, chmod +x): `cd "$(dirname "$0")"` → `./mvnw -q clean package -DskipTests` (fallback `mvn`) → `docker compose build` → `docker compose up -d` → print `http://localhost:8080`.

## Test

`KafkaDemoIntegrationTest`: `@SpringBootTest` + `@EmbeddedKafka(topics="messages", partitions=3)` + `@TestPropertySource(properties="spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")`. Send via `KafkaTemplate`, assert `MessageStore` receives it (Awaitility — ships with spring-boot-starter-test ≥3.2).

## Implementation order

1. Scaffold: pom.xml, .gitignore, `mvn wrapper:wrapper`, main class
2. docker-compose.yml → bring up 3 brokers → verify with `kafka-topics.sh --list` (isolates the fiddly part)
3. application.yml + application-local.yml + KafkaTopicConfig
4. Producer path → verify with curl + console consumer
5. Consumer path → verify via `docker compose logs -f app`
6. UI (view controller, index.html, style.css, MessageFeed, SSE endpoint)
7. Dockerfile, .dockerignore, build-deploy.sh → full `./build-deploy.sh`
8. @EmbeddedKafka test

## Verification (end-to-end)

1. `./build-deploy.sh` → `docker compose ps` shows 3 brokers healthy + app up
2. `curl -X POST http://localhost:8080/api/messages -H 'Content-Type: application/json' -d '{"content":"first"}'` → `{"key":1,"partition":…,"offset":…}`
3. Open `http://localhost:8080`, send several messages — each appears live via SSE; partition column varies across 0/1/2
4. Broker-level proof: `docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --describe --topic messages` (3 partitions, RF 3, leaders spread); `kafka-console-consumer.sh … --from-beginning --property print.key=true`; `kafka-consumer-groups.sh … --describe --group kafka-demo-group` (lag 0)
5. `./mvnw test` — embedded-broker round trip passes without docker

## Pitfalls (verify during implementation)

Missing `CLUSTER_ID` (image throws) · no ZooKeeper-era vars · every listener name in security map · INTERNAL advertised hostnames must be service names not localhost · controller vs EXTERNAL port collisions · `NewTopic` replicas=3 fails until 3 brokers healthy (hence healthchecks + depends_on) · embedded broker needs replicas=1 · `local` profile never inside docker.

---

# Phase 3 — JSON payloads · idempotent consumer · retry & DLT

**Status: DONE** — implemented, all 3 integration tests green, docs 06/07/09/README updated. Full live verify is the last step.

## What changed

| Piece | Files | Behavior |
|---|---|---|
| Wire format | `domain/MessagePayload.java` | value on the wire is `MessagePayload(id, content, sentAt)` as JSON (String serializer unchanged) |
| Producer | `MessageProducer` | `ObjectMapper` + `ProducerRecord` with `schema-version: 1` header |
| Idempotency | `consumer/DedupStore.java` | bounded set of processed `MessagePayload.id`s; mark only *after* success → duplicates logged "Duplicate skipped" |
| Retry chain | `MessageConsumer` | `@RetryableTopic(attempts="4", numPartitions="3", backoff=1s×2, exclude=JsonProcessingException)` → topics `messages-retry-1000/2000/4000` → `messages-dlt` |
| DLT handling | `@DltHandler` on `MessageConsumer` | reads `kafka_exception-cause-fqcn` (root cause) header → `DltStore` → UI + SSE `dlt` event |
| Poison trigger | `MessageConsumer` | content containing `"boom"` always throws → retries → DLT |
| UI | `index.html`, `MessageFeed`, `MessageViewController` | ☠ poison button + DLT table (server-rendered + live via SSE) |

## Test results (embedded broker)

```
Tests run: 3, Failures: 0, Errors: 0  —  BUILD SUCCESS
  1. producedMessageIsConsumedIntoStore   (JSON round trip via MessageProducer)
  2. poisonMessageRetriesThenLandsInDlt   (retries exhaust → DLT, root cause = RuntimeException)
  3. duplicateDeliveryIsSkipped           (same payload id delivered twice → processed once)
```

Notes:
- `@DirtiesContext(AFTER_EACH_TEST_METHOD)` isolates each test's in-memory stores.
- The DLT record carries `kafka_exception-fqcn` (framework wrapper) **and** `kafka_exception-cause-fqcn` (real cause) — the handler prefers the cause, so the UI shows `java.lang.RuntimeException`, not the wrapper.
- The DLT listener's own error handler would re-publish to `messages-dlt` if `@DltHandler` throws — the header fix (above) is what prevents that infinite loop.

## Live verify (pending, next)

1. `./build-deploy.sh` → `docker compose ps` all healthy
2. POST normal → UI table via SSE; broker replay shows JSON values
3. POST `boom` → logs show retries → `messages-dlt`; UI DLT table shows it (error `java.lang.RuntimeException`)
4. `kafka-topics.sh --list` → `messages-dlt`, `messages-retry-1000/2000/4000`
5. Replay dedup: `--reset-offsets --to-earliest` → UI doesn't duplicate, logs show "Duplicate skipped"
