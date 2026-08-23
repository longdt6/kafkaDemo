# kafkaDemo — Spring Boot as a Kafka Client (producer + consumer)

A hands-on learning project: one **Java 21 / Spring Boot** app acts as both a Kafka **producer** and **consumer** against a local **3-node KRaft Kafka cluster**, with a Thymeleaf web UI. Type a message → it goes REST → producer (JSON payload + `schema-version` header) → Kafka → consumer (`@RetryableTopic` + idempotent dedup) → back to your screen live via SSE. Poison messages retry with backoff, land in `messages-dlt`, and show up in the UI's DLT table.

```
Browser ──HTTP──> Spring Boot (8080)
                     │  POST /api/messages          (producer → INTERNAL listeners)
                     ▼
              kafka-1 · kafka-2 · kafka-3   (apache/kafka:3.9.2, KRaft, no ZooKeeper)
                     ▲
                     │  @KafkaListener on topic "messages"  (consumer → MessageStore, dedup)
                     │  @RetryableTopic → messages-retry-* → messages-dlt
              Spring Boot (same app)  ──SSE──>  page + DLT table update live
```

## What this project teaches

Everything in [`docs/`](docs/) — the learning material is the point of this repo, and each doc follows **concept → official Kafka docs → hands-on in this project**:

| Doc | Topic |
|---|---|
| [01](docs/01-kafka-basics.md) | The log, topics, partitions, offsets, ISR, KRaft vs ZooKeeper |
| [02](docs/02-docker-compose-kraft-cluster.md) | The 3-node KRaft docker-compose, env-var by env-var + common errors |
| [03](docs/03-producer-deep-dive.md) | Async send, acks, retries, idempotence, key partitioning |
| [04](docs/04-consumer-deep-dive-groups.md) | Consumer groups, offsets, auto-commit, auto-offset-reset, rebalance |
| [05](docs/05-spring-kafka-config.md) | `application.yml` → producer/consumer/admin configs, profiles |
| [06](docs/06-serialization.md) | Serializers, JSON + Jackson, headers, poison pills |
| [07](docs/07-error-handling-retry-dlt.md) | Producer vs consumer retries, error handler, dead-letter topic |
| [08](docs/08-topics-partitions-scaling.md) | Partitions & parallelism, RF, `min.insync.replicas` |
| [09](docs/09-kafka-cli-verification.md) | The Kafka CLI toolkit, adapted to the `apache/kafka` image |

## Quick start

Prereqs: Docker, Java 21+ (to run tests), Maven (or use the wrapper).

```bash
# Build the jar, build images, start 3 brokers + the app
./build-deploy.sh

# Open the UI
open http://localhost:8080
```

You can also run the app on your Mac against the brokers only (no dockerized app):

```bash
docker compose up -d kafka-1 kafka-2 kafka-3     # brokers only
./mvnw spring-boot:run -Dspring-boot.run.profiles=local
```

## Verify it works

```bash
curl -X POST http://localhost:8080/api/messages \
     -H 'Content-Type: application/json' -d '{"content":"hello kafka"}'
# → {"key":1,"partition":0,"offset":0}        (partition/offset vary)
```

Then open the UI and watch the message appear within ~1s. Or inspect the broker directly — see [09](docs/09-kafka-cli-verification.md) for the full script.

## Layout

```
docker-compose.yml     3-node KRaft cluster + the app (all in one compose)
build-deploy.sh        jar → image → docker compose up (one command)
Dockerfile             multi-stage (maven → eclipse-temurin:21-jre)
src/main/java/io/github/kafkademo/
  producer/MessageProducer.java   JSON MessagePayload + schema-version header (whenComplete)
  consumer/MessageConsumer.java   @RetryableTopic + @DltHandler → MessageStore/DltStore + SSE
  consumer/DedupStore.java        idempotent dedup by MessagePayload.id
  feed/MessageFeed.java           SseEmitter broadcast (live UI: message + dlt events)
  web/…                           REST API + Thymeleaf controller
src/main/resources/
  application.yml         default profile = inside docker (INTERNAL listeners)
  application-local.yml   host profile = localhost:9092,9093,9094 (EXTERNAL)
docs/                    the learning material (start with 01); the build plan lives in [PLAN.md](docs/PLAN.md)
```

## Common commands

| Do this | Command |
|---|---|
| Full deploy | `./build-deploy.sh` |
| App logs | `docker compose logs -f app` |
| Stop everything | `docker compose down` |
| Wipe cluster data | `docker compose down -v` |
| Run tests (embedded broker) | `./mvnw test` |
