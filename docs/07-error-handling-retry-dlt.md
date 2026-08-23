# 07 — Error Handling, Retries, and the Dead-Letter Topic

> Two very different retry systems live on opposite sides of the broker. Confusing them is the most common source of "but I set retries and it still failed" confusion. This doc separates them and then shows the standard consumer-side arsenal: retry topics and the **dead-letter topic (DLT)** — the pattern the course's `library-events.RETRY` / `library-events.DLT` exercise.

---

## 1. Producer-side retries vs consumer-side retries

| | **Producer** (`spring.kafka.producer.properties.*`) | **Consumer** (`spring.kafka.listener.*` + `DefaultErrorHandler`) |
|---|---|---|
| What's retried | **Sending** the batch to the broker | **Processing** a delivered record |
| Why | transient broker errors (leader election, `NotLeader`, timeouts) | your listener threw (validation, DB down, deserialization) |
| Config | `retries`, `retry.backoff.ms`, `retry.backoff.max.ms` | `DefaultErrorHandler` max attempts / back-off, or `@RetryableTopic` |
| Success condition | broker acked | listener returned without exception |
| Failure end-state | future completes exceptionally → your `whenComplete` logs it | record skipped (with DLT) or the container keeps retrying forever |

**Producer retries do not re-process anything** — they re-*send*. **Consumer retries do not re-send anything** — they re-process an already-committed record. Keep that boundary clear and most Kafka debugging gets easier.

---

## 2. The default consumer behavior (no error handler configured)

Spring's modern default error handler is **`DefaultErrorHandler`**. Its behavior:

1. The listener throws.
2. The container seeks the partition back to that record and **retries** (default max 10 attempts, with an increasing backoff; the record's delivery attempt is tracked via a retry header).
3. After max attempts, it **sends the record to a DLT** — `<topic>.<group>.DLT` by default — and **commits the offset past it**, so the poison record doesn't stall the partition.
4. The DLT is a *normal topic*; you can attach a second `@KafkaListener` to it for inspection, alerts, or manual repair.

```yaml
# Tune the number of retries (Spring, not the Kafka client):
spring:
  kafka:
    listener:
      # DefaultErrorHandler defaults: 10 attempts; override via a bean instead of yml for backoff
```

The old Spring name you'll still see in tutorials — `SeekToCurrentErrorHandler` — is **deprecated**; `DefaultErrorHandler` replaced it (Spring Kafka 2.8+).

### Important nuance: recovery happens *before* commit
Because the consumer's offset is only committed after the listener succeeds (or after DLT recovery), an unhandled poison record would be **re-fetched forever** on restart. The `DefaultErrorHandler` + DLT is what breaks that loop — it's your safety valve. **This is the single most important consumer setting to understand in production.**

---

## 3. The retry-topic pattern — `@RetryableTopic`, now implemented here

This project uses Spring Kafka's **`@RetryableTopic`** on `MessageConsumer`. When the listener throws, the record walks an increasing-delay retry chain, then lands in the DLT. The actual topics (auto-created with the suffix strategy `SUFFIX_WITH_DELAY_VALUE`) are:

```
main topic "messages"
   │  listener throws (e.g. content contains "boom")
   ▼
"messages-retry-1000"  (wait 1s,  retry)  ─┐ exhausted →
"messages-retry-2000"  (wait 2s,  retry)  ─┤ each attempt delays 2x longer
"messages-retry-4000"  (wait 4s,  retry)  ─┘
   ▼ finally exhausted
"messages-dlt"         (the dead letter — shown in the UI table, ops inspect)
```

The real annotation on `MessageConsumer`:

```java
@RetryableTopic(
    attempts = "4",                       // original + 3 retries
    numPartitions = "3",
    backoff = @Backoff(delay = 1000, multiplier = 2.0),
    exclude = { JsonProcessingException.class })   // bad JSON → straight to DLT
@KafkaListener(topics = "${spring.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
public void onMessage(ConsumerRecord<Integer, String> record) { ... }

@DltHandler
public void onDlt(ConsumerRecord<Integer, String> record,
        @Header(KafkaHeaders.EXCEPTION_CAUSE_FQCN) String cause, ...) { ... }
```

Notes on the real behavior:
- **Topic names use *dashes* and the delay in ms**: `messages-retry-1000/2000/4000`. (The course's hand-rolled `library-events.RETRY`/`library-events.DLT` uses *dots* — different convention, same idea.) DLT = **`messages-dlt`**.
- **`numPartitions = "3"`** so the retry/DLT topics are readable by our 3-partition consumer layout; the default replication factor (`-1` → broker default = 1) is fine for scratch topics and for the single-node embedded test broker.
- **`exclude = { JsonProcessingException.class }`** — a *permanent* failure (message isn't valid `MessagePayload` JSON) skips the retry chain entirely and goes straight to the DLT. Retrying a parse error just burns cycles (the docs/07 §3 "when NOT to use it" point, now enforced in code).
- **`@DltHandler`** receives the DLT record plus the exception headers — `kafka_exception-fqcn` (the wrapper), `kafka_exception-cause-fqcn` (the real cause — what we display), and the stack trace. Try `kafka-consumer-groups.sh --list` after a poison: you'll see `kafka-demo-group-dlt` and `kafka-demo-group-retry-*` — the retry chain is just more consumer groups.
- The exception a poison produces shows as `java.lang.RuntimeException` in the DLT table (the root cause), not the framework's `ListenerExecutionFailedException` wrapper.

Why a chain instead of one topic? So a downstream transient failure (DB down for 10s) can be retried with **increasing delay** while keeping main-topic traffic flowing, and only genuinely bad records reach the DLT.

**When NOT to use it:** when the failure is *guaranteed permanent* (serialization/validation), retries just burn cycles — configure the error handler to go straight to DLT, or use `@DltHandler`. That's exactly the `exclude = JsonProcessingException` above.

---

## 4. Consumer idempotency: why duplicates happen and how to kill them

With at-least-once delivery (see `03`, `04`), the *same logical message* can be processed twice: producer retry re-delivers a batch, or the consumer crashes between processing and offset commit. Retry systems make duplicates *more* likely, not less. The standard fixes (pick based on your data):

- **Natural idempotency** — the operation is safe to repeat (e.g. upsert with the same key).
- **Dedup store** — keep processed message IDs (a Redis set, a DB unique constraint on the message id) and skip already-seen ones. **Implemented here as `DedupStore`**: the `MessagePayload.id` (a UUID from the producer) is the dedup key; after a record is processed successfully the id is marked, and any re-delivery of the same id is logged as *"Duplicate skipped"* and ignored. Marking happens *after* success, so a failed attempt (one that goes to the DLT) is never marked and could legitimately be reprocessed if you repair it.
- **Kafka transactions** (exactly-once semantics) — the heavyweight solution, out of scope here but the reason `enable.idempotence` + transactional producers exist.

**Rule:** assume every message can arrive twice; make consumption harmless on replay.

---

## 5. Where each knob lives in this project

| Concern | Where to configure |
|---|---|
| Producer send retries / backoff / idempotence | `application.yml` → `spring.kafka.producer.properties.*` (see `03`) |
| Producer async failure logging | `MessageProducer.whenComplete` |
| Consumer retry chain | **`@RetryableTopic` on `MessageConsumer`** (attempts=4, backoff 1s×2, exclude JsonProcessingException) |
| DLT topic name | `messages-dlt` (default `-dlt` suffix; dashes not dots) |
| DLT listener (inspect bad records) | **`@DltHandler on `MessageConsumer`** → `DltStore` → UI table + SSE |
| Consumer-side idempotency | **`DedupStore`** (dedup by `MessagePayload.id`), marked only after success |

---

## 6. Official references

- [Spring for Apache Kafka — Error Handling](https://docs.spring.io/spring-kafka/reference/kafka/error-handling.html) (`DefaultErrorHandler`, `DeadLetterPublishingRecoverer`)
- [Spring for Apache Kafka — RetryTopic](https://docs.spring.io/spring-kafka/reference/retrytopic/index.html) (`@RetryableTopic`, `@DltHandler`)
- [Kafka documentation — Exactly-once semantics](https://kafka.apache.org/documentation/#semantics)
- The course repo: `LibraryEventsConsumerManualOffset`, `RetryScheduler`, `FailureRecord` — the hand-rolled version of this whole doc

---

## 7. Hands-on in this project

The poison trigger is already in the code — a `MessagePayload` whose content contains **"boom"** always throws. No code edit needed.

```bash
# 1. Send a poison from the UI (☠ button) or:
curl -s -X POST http://localhost:8080/api/messages \
     -H 'Content-Type: application/json' -d '{"content":"boom please fail"}'

# 2. Watch the retry chain + DLT in the app logs (3 retries, growing delay, then dead-letter):
docker compose logs -f app | grep -E "Retrying|Dead-lettered|RuntimeException"

# 3. See the generated topics (note the -retry-<delay> and -dlt names):
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list
#    messages / messages-dlt / messages-retry-1000 / messages-retry-2000 / messages-retry-4000

# 4. The record sits in the DLT, and the UI's "Dead-letter topic" table shows it live:
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:19092 --topic messages-dlt --from-beginning --property print.value=true

# 5. Non-JSON values go STRAIGHT to the DLT (exclude = JsonProcessingException):
echo "not json at all" | docker exec -i kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:19092 --topic messages

# 6. Duplicate/replay proof (dedup): reset the group so the consumer re-reads history —
#    the UI does NOT double up rows, and logs show "Duplicate skipped":
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 \
  --group kafka-demo-group --reset-offsets --to-earliest --execute
```

**Observation to make:** the difference between a *transient* failure (fix the cause, the record succeeds on retry, no DLT) and a *poison* failure (always throws, lands in `messages-dlt`). And that a permanent failure (bad JSON) bypasses the retry chain entirely — retries are for transient things, DLT is for everything that survives.

---

**Next:** [08 — Topics, partitions & scaling](08-topics-partitions-scaling.md)
