# 03 — Producer Deep-Dive

> In this project the producer is `MessageProducer`, called from `MessageApiController` when you POST to `/api/messages`. Every knob below maps to something in `application.yml` or that class.

---

## 1. The KafkaTemplate

Spring Kafka wraps the raw `KafkaProducer` in a **`KafkaTemplate<Integer, String>`** — the generic parameters are `(keyType, valueType)`. It's auto-configured by Spring Boot from `spring.kafka.*` properties, so you never construct a producer yourself:

```java
@Service
public class MessageProducer {
    private final KafkaTemplate<Integer, String> kafkaTemplate;
    private final AtomicInteger keyCounter = new AtomicInteger();

    public MessageProducer(KafkaTemplate<Integer, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<SendResult<Integer, String>> send(String content) {
        Integer key = keyCounter.incrementAndGet();
        return kafkaTemplate.send(topic, key, content)
            .whenComplete((result, ex) -> {
                if (ex != null) log.error("Failed: {}", ex.getMessage());
                else log.info("Sent -> partition={} offset={}", result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            });
    }
}
```

`KafkaTemplate.send(...)` is **asynchronous** — it returns a `CompletableFuture<SendResult>` immediately. The `.whenComplete` callback logs the actual `partition` and `offset` from the broker's response. The key is an incrementing integer, which routes messages round-robin-ish across partitions (see §4).

### Send variants you'll meet

| Method | Meaning |
|---|---|
| `kafkaTemplate.send(topic, key, value)` | explicit topic + key (what we use) |
| `kafkaTemplate.send(topic, value)` | no key → `null` key → **round-robin** across partitions |
| `kafkaTemplate.send(ProducerRecord)` | full record: partition, key, value, timestamp, **headers** |
| `kafkaTemplate.sendDefault(key, value)` | uses `spring.kafka.template.default-topic` — the course's pattern |

---

## 2. acks, retries, idempotence — the reliability triangle

These three settings in `application.yml` work *together*:

```yaml
producer:
  properties:
    acks: all               # leader + all in-sync replicas ack
    retries: 10
    retry.backoff.ms: 1000
    enable.idempotence: true
```

### `acks`
Controls when the broker acknowledges a send (see the table in `01`, §5):
- `acks=0` — fire-and-forget. Fastest, no ack, **can lose messages silently** (broker offline, serialization error, etc.).
- `acks=1` — the partition **leader** wrote it. Default in old Kafka; one point of failure (leader dies before followers replicate).
- `acks=all` — every **in-sync** replica wrote it. Strongest guarantee; the broker will also retry internally if the ISR is small.

### `retries` + `retry.backoff.ms`
Transient errors (leader election, `NotLeaderOrFollower`) are retried automatically. `retries: 10` with `retry.backoff.ms: 1000` = try 10 times, waiting 1s between attempts, before the future completes exceptionally. The `whenComplete` handler then sees the failure and logs it. Without retries, a brief leader election would fail the send even though the message could have been delivered moments later.

### `enable.idempotence: true`
**What it prevents:** the classic Kafka duplicate problem. Producer sends a message, broker commits it but the ack is lost; producer retries and the message is written **twice**. With idempotence on, the producer tags every batch with a sequence number and the broker **dedupes** repeated retries of the same batch. This makes at-least-once delivery actually safe to lean on.

**Constraint:** idempotence **requires `acks=all`** (that's why the config has both). The Kafka client silently enforces this — if you set idempotence with a weaker acks, it upgrades you to `all`.

### What this buys you
With `acks=all` + `retries` + idempotence: once `send()` completes successfully, **the message is durably committed and will not be lost**. You still may see *duplicates on the consumer side* after a retry of an already-committed batch — that's inherent to at-least-once and handled by consumer-side dedup/idempotency (see `07`).

---

## 3. Blocking vs non-blocking send

`MessageApiController` waits on the future so the HTTP response can show you the partition and offset:

```java
SendResult<Integer, String> result = producer.send(request.content()).get(5, TimeUnit.SECONDS);
return ResponseEntity.ok(Map.of(
    "key", result.getProducerRecord().key(),
    "partition", result.getRecordMetadata().partition(),
    "offset", result.getRecordMetadata().offset()));
```

- **`.get(5s)`** blocks the HTTP thread until the broker acks → you *see* the result in the response. Fine for a local, single-user demo; bad for production (a slow broker would tie up a request thread).
- **Production pattern:** return `202 Accepted` immediately and log/handle success and failure in `whenComplete` — never block the web thread on I/O.

Both are worth knowing; the demo deliberately uses the blocking form because the learning payoff (partition/offset in the response) is worth the small anti-pattern.

---

## 4. Partitioning: keys, hash, and round-robin

When the record has a **key**, the partition is chosen as `murmur2(key) % numPartitions`. Consequences:

- **Same key → same partition → strict ordering** for that key. This is the entire reason Kafka has keys — think `customerId`, `orderId`, `deviceId`.
- **Different keys → spread** across partitions (good distribution = good parallelism downstream).
- Changing the key (or the partition count) **breaks the key→partition mapping**, so ordering guarantees only hold *within the current partition layout*.

With **no key** (`null`), the partition is chosen **round-robin** — even spread, no ordering.

In this demo the key is a counter (1, 2, 3, …), so `murmur2` spreads them across partitions 0–2. POST 6 messages and watch the partition column in the UI cycle through all three. If we'd used a constant key, every message would land on the **same** partition — a great experiment to run to *feel* the difference.

---

## 5. Batch size, linger, and buffer — how throughput is bought

The producer is **not** a per-message system. It accumulates records in per-partition buffers and sends **batches**:

- **`linger.ms`** — how long to wait for more records before sending a full-or-partial batch. Default 0 (send immediately). Raising it to ~5–10ms dramatically increases batch sizes and throughput at the cost of a few ms latency. **Good to tune for learning.**
- **`batch.size`** — max bytes in a batch (default 16 KB).
- **`buffer.memory`** — total buffer across partitions (default 32 MB). If the producer falls behind the broker, `send()` starts blocking, then throws `BufferExhaustedException` — a classic "my producer is slow" failure.
- **`max.request.size`** — largest single message/batch (default 1 MB). Sending anything bigger fails with a clear error.

Because the producer batches, `ack` is per-*batch*, not per-message — which is why idempotence is per-batch too.

---

## 6. Official references

- [Kafka producer configuration](https://kafka.apache.org/documentation/#producerconfigs) — the definitive list (acks, retries, enable.idempotence, linger.ms, …)
- [Kafka documentation — Exactly-once semantics](https://kafka.apache.org/documentation/#semantics) — the acks/retries/idempotence story, at-least-once vs exactly-once
- [Spring for Apache Kafka — KafkaTemplate](https://docs.spring.io/spring-kafka/reference/kafka-template.html)
- [Spring Boot reference — Kafka producer properties](https://docs.spring.io/spring-boot/reference/messaging/kafka.html)

---

## 7. Hands-on in this project

```bash
# 1. Send several messages through the API
for i in 1 2 3 4 5 6; do
  curl -s -X POST http://localhost:8080/api/messages \
       -H 'Content-Type: application/json' -d "{\"content\":\"msg $i\"}"
  echo
done

# 2. Watch the producer log: each line shows partition + offset
docker compose logs -f app | grep "Sent"

# 3. Prove ordering with keys: watch a console consumer print key + partition
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:19092 --topic messages --from-beginning \
  --property print.key=true --property print.partition=true

# 4. Experiment: change send() to use a CONSTANT key, rebuild, resend.
#    Every message now lands on ONE partition — you'll see it in the UI immediately.
```

**Observation to make:** the UI's partition column. With the incrementing key it cycles 0→1→2; with a constant key it pins to one partition. That's §4 made visible.

---

**Next:** [04 — Consumer deep-dive & consumer groups](04-consumer-deep-dive-groups.md)
