# 04 — Consumer Deep-Dive & Consumer Groups

> In this project the consumer is `MessageConsumer` (`@KafkaListener`) writing into `MessageStore` and pushing to the SSE feed. Every concept below is visible in the UI's message table or via the CLI.

---

## 1. The consumer is just an offset tracker

The critical mindset shift from a traditional queue: **consuming does not delete anything.** A consumer is a process that reads the log *at a position it tracks itself*. The data stays; the consumer's *offset* moves. This is why:
- A new consumer can replay all history (`auto-offset-reset: earliest` + seek to 0).
- Old consumers' offset state lives in a Kafka internal topic (`__consumer_offsets`), not in the consumer's memory — so a consumer can die and come back at the same offset.
- Retention, not consumption, is what eventually removes data.

Spring's `@KafkaListener` hides most of the offset plumbing, but you must understand what's underneath to debug it (see §5 on `auto-offset-reset` and `kafka-consumer-groups`).

---

## 2. Consumer groups and partition assignment

Consumers with the same **`group.id`** form a **consumer group**. Kafka's group coordinator assigns each partition to **exactly one active member** of the group.

```
Topic "messages" (3 partitions)          Consumer group "kafka-demo-group"
partition 0  ────────────────►  consumer A   (this project: one @KafkaListener)
partition 1  ────────────────►  consumer A
partition 2  ────────────────►  consumer A
```

**The rules:**
- Every message in a partition is delivered to exactly **one** member per group (no duplicate delivery *within* a group from a single partition).
- **Order within a partition is preserved** to that one member — this is why ordering survives a group.
- A group with **more members than partitions** has idle members (no work). Partitions cap parallelism: to process faster, add *partitions* (see `08`), not just members.
- Different groups are fully independent: two groups both read **every** message. Groups are how you do pub/sub over the same topic (e.g. "audit" group + "ui" group).

### This demo is a single-consumer group
Our one `@KafkaListener` with `group-id: kafka-demo-group` is assigned **all 3 partitions**. To *see* scaling: temporarily set `spring.kafka.consumer.` concurrency via `@KafkaListener(concurrency = "3")` — Spring runs 3 listener containers, each grabbing a partition — or (later) run a second app instance with the same group id and watch partitions split between the two. The UI already makes this visible because each consumed message records its `partition`.

---

## 3. Offsets: committed vs position, auto-commit

A consumer has **two** offset-like numbers per partition:
- **Position** — where it *will* read next (in-memory, advances as records are polled).
- **Committed offset** — the offset it has *reported* to the broker (`__consumer_offsets`), used when it re-joins the group to know where to resume.

Spring Boot defaults to **auto-commit**: every ~5s (or on close), the container commits the current position. Two consequences worth internalizing:

- **At-least-once by default.** The commit can lag behind processing (e.g. 5s window), so a crash between *process* and *commit* → reprocess from the committed offset → **duplicate handling**. Consumers must be idempotent or dedup (see `07`).
- **`enable.auto.commit` can be disabled** and commits moved into your code (`Acknowledgment` or `KafkaOperations` in Spring) for at-most-once or exactly-once-ish control. That's the *manual* / *acknowledgement* modes in the course (`LibraryEventsConsumerManualOffset`).

Spring's default container **acks on the consumer thread** after the listener returns; you can switch to `MANUAL`/`MANUAL_IMMEDIATE` ack modes for finer control.

---

## 4. Rebalance

When a member joins, leaves, or dies (heartbeat timeout), or a partition is added, the group **rebalances**: the coordinator reassigns partitions to the remaining members. During a rebalance, **no member consumes** — the group is briefly paused.

- **Eager rebalance** (older default): all members revoke everything, then get new assignments. Simple, but "stop the world" — and **offsets during the revoke** matter.
- **Cooperative rebalance** (KIP-429, newer default): members keep partitions that aren't being reassigned and only give up the ones that move. `spring.kafka.consumer.properties.partition.assignment.strategy: CooperativeStickyAssignor` enables it in Spring.
- **`GROUP_INITIAL_REBALANCE_DELAY_MS: 0`** in our compose removes the startup delay — your first message shows up instantly (see `02`).

If you run a second app instance (same `group-id`) the rebalance is *visible*: watch `docker compose logs` as the partitions split 3 → split 1.5/1.5.

---

## 5. auto-offset-reset — where a *new* group starts

When a consumer group has **no committed offset** for a partition (brand-new group, or offset expired by retention), `auto-offset-reset` decides:

| Value | New group starts at | Use case |
|---|---|---|
| `latest` (ours) | **the end of the log** — only *new* messages | dashboards, live UI — don't replay history |
| `earliest` | **the beginning** — full history | jobs that must process everything (ETL, audit) |

**Only applies when there's no committed offset.** Once the group commits, this setting is ignored (resume-from-committed wins). A classic gotcha: you restart the app expecting history, but the group already committed → you get nothing new because `latest` started you at the end. Fix: `kafka-consumer-groups.sh --to-earliest --execute` (see `09`) or delete the group.

`auto-offset-reset` is also **not** a "start over" switch after the first run — it is *first-run* behavior only.

---

## 6. Lag — the operational health metric

**Lag = the newest committed offset in a partition minus the committed consumer offset** for that partition. It's "how far behind is this group." The demo's `MessageStore` caps at 100 messages, but the *broker* keeps everything (retention). If a consumer dies, lag grows; when it returns, it replays everything behind it in order.

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:19092 --describe --group kafka-demo-group
```

Output: one row per partition with `CURRENT-OFFSET`, `LOG-END-OFFSET`, and `LAG`. When the demo is idle, lag = 0 — producer and consumer are perfectly in sync. Post a message and watch lag flick to 0 again within a second. **This command is your single best health check.**

---

## 7. Official references

- [Kafka documentation — Consumers and consumer groups](https://kafka.apache.org/documentation/#intro_consumers)
- [Kafka documentation — Consumer configs (`auto.offset.reset`, `enable.auto.commit`, `group.id`)](https://kafka.apache.org/documentation/#consumerconfigs)
- [KIP-429 — KafkConsumer group rebalance protocol](https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+KafkConsumer+Group+Reduction+and+Rebalance) (cooperative rebalancing)
- [Spring for Apache Kafka — @KafkaListener & containers](https://docs.spring.io/spring-kafka/reference/kafka-container.html)
- [Spring Boot reference — Kafka consumer properties](https://docs.spring.io/spring-boot/reference/messaging/kafka.html)

---

## 8. Hands-on in this project

```bash
# 1. Send a few messages (see 03), then inspect the group
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:19092 --describe --group kafka-demo-group

# 2. See the group's offsets topic (metadata about your consumers)
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --describe --topic __consumer_offsets

# 3. Watch a rebalance happen: stop the app, then start it again
docker compose stop app && docker compose start app
#    → logs show the listener joining the group and re-assigning partitions

# 4. Experiment with earliest vs latest: stop app, delete the group, restart.
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:19092 --delete --group kafka-demo-group
#    With auto-offset-reset: earliest, the UI suddenly shows ALL history from the beginning.
```

**Observation to make:** the difference between `latest` (start empty) and `earliest` (start at the beginning) after deleting the group. That single experiment makes §5 concrete.

---

**Next:** [05 — Spring Kafka configuration, yml → config](05-spring-kafka-config.md)
