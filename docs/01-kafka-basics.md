# 01 — Kafka Basics: The Log, Topics, Partitions, Offsets

> Every file in `docs/` follows the same shape: **concept** → **official reference** → **hands-on in this project**. This is the foundation — the other docs build on it.

---

## 1. What Kafka actually is

Kafka is not a traditional message queue with a queue at the front and a database at the back. It is an **append-only, distributed commit log**. You can think of it as a replicated filesystem for messages: producers append to the end, consumers read from an offset they track themselves, and messages are never deleted because a consumer "acked" them — they are deleted by **retention** (time or size), or not at all.

**Why this matters for you as a client:** almost every surprising behavior of Kafka flows from this design. Messages are *replayable* (a consumer can re-read old data). Consumers don't remove data. Producers and consumers are fully decoupled — a producer doesn't care who reads the message.

Jay Kreps' classic essay *The Log* (2013) is the best explanation of why the log is the primitive every data system is built on: databases (write-ahead logs), MySQL binlog, Kafka — all the same abstraction. If you read one thing for background, read that.

---

## 2. Topics, Partitions, Offsets

### Topic
A **topic** is a named, ordered log of messages. In this project the topic is `messages`. A topic is a *logical* concept; it is not stored in one place — it is split into partitions.

### Partition
A topic is split into **partitions**, each of which is an *independent, append-only log* with its own order.

```
Topic "messages" (3 partitions)

partition 0:  [msg][msg][msg][msg]
partition 1:  [msg][msg][msg]
partition 2:  [msg][msg][msg][msg][msg]
```

- **Partitions are the unit of parallelism.** A topic with N partitions can be read by up to N consumers in a group (each takes one partition) and written in parallel.
- **Order is only guaranteed *within* a partition**, never across the topic. If you need ordering for a key (e.g. all events for `customerId=42` in order), that key must map to the same partition.
- The number of partitions **cannot be decreased** and increasing it is a one-way door — choose carefully (see `08-topics-partitions-scaling.md`).

### Offset
An **offset** is the position of a message within a partition — a monotonically increasing integer (0, 1, 2, …), unique within that partition. It is *not* a byte position; it always increments by 1 per message, regardless of message size. A message is uniquely identified by `(topic, partition, offset)`.

When you POST a message in this demo, the response includes `partition` and `offset` — those are the message's coordinates in the log.

### Key
A producer can attach a **key**. Kafka hashes the key (`murmur2(key) % numPartitions`) to pick the partition, so **all messages with the same key go to the same partition, in order**. Messages with different keys spread across partitions. In this demo the key is an incrementing integer, so messages cycle across the 3 partitions (watch the partition column in the UI).

---

## 3. Log segments, replication, ISR

### Segments
A partition's log is not one file — it's a sequence of **segment files**, rolled when a size threshold (`log.segment.bytes`, default 1 GiB) or time threshold is hit. Segments matter to you as a client mainly because:
- Retention deletes whole segments (cheap) rather than individual messages (expensive).
- The **active segment** is the one currently receiving writes; only active segments accept appends.

### Replication
Each partition is replicated across brokers. One broker is the **leader** (all reads/writes go through it), the others are **followers** that copy the leader's log. Replication factor (RF) = number of copies.

### ISR — In-Sync Replicas
Followers that have caught up with the leader (within `replica.lag.time.max.ms`) form the **in-sync replica set (ISR)**. This matters to producers and consumers because:
- `acks=all` means "leader + **all in-sync replicas** have written the message" — not all replicas, just the ones currently caught up.
- Consumers can only read **committed** records, i.e. those written to the ISR.
- If a broker dies, the partition's new leader is chosen from the ISR (never from an out-of-sync replica), so no data already committed is lost.

```
partition 0 of "messages":  leader = kafka-1
                            followers = kafka-2, kafka-3
                            ISR = [kafka-1, kafka-2, kafka-3]   (all caught up)
```

### High watermark
The **high watermark** is the offset up to which messages are considered "committed" (replicated to the ISR). Consumers only see messages up to the high watermark — so even a producer that got `acks=1` may have its message invisible to consumers until it's in the ISR.

---

## 4. KRaft vs ZooKeeper (why no ZooKeeper here)

Historically Kafka used **ZooKeeper** to store cluster metadata (which broker is controller, partition leaders, configs, etc.). Since Kafka **3.3**, ZooKeeper is optional; since Kafka **4.0** it is removed entirely. The replacement is **KRaft** — the *Kafka Raft* consensus protocol that runs *inside* Kafka itself.

In KRaft mode, some nodes act as **controllers** (the metadata quorum, using Raft consensus) and some as **brokers** (data). A node can be **combined** — both broker and controller — which is exactly what this project's 3-node compose file does (`KAFKA_PROCESS_ROLES: broker,controller`).

Why this matters for a client:
- **No external dependency** — one system to run, not Kafka + ZooKeeper.
- The 3 nodes run their own metadata quorum; if one dies, the other two still elect a controller.
- The old `cp-kafka` + `cp-zookeeper` tutorials you may have seen (including the course repo's `docker-compose-multi-broker.yml`) are the *previous* generation. The `apache/kafka` image + KRaft env vars (`KAFKA_NODE_ID`, `KAFKA_PROCESS_ROLES`, `KAFKA_CONTROLLER_QUORUM_VOTERS`) are the current one. See `02-docker-compose-kraft-cluster.md`.

---

## 5. Delivery semantics at a glance

The `acks` setting on the producer is the primary knob for delivery guarantees (deep dive in `03-producer-deep-dive.md`):

| `acks` | Guarantee | Typical use |
|---|---|---|
| `0` | fire-and-forget, no acknowledgement | metrics where loss is fine |
| `1` | leader wrote it | throughput-sensitive, single point of failure |
| `all` | leader + all in-sync replicas wrote it | **default in this project** — no committed-message loss |

Combined with producer retries + idempotence (`enable.idempotence=true`), `acks=all` gives **at-least-once** delivery: a message is never lost once acknowledged, but the consumer may see duplicates after a retry. True **exactly-once** is a separate topic (transactions) — out of scope here but named in `08` for future study.

---

## 6. Official references

- [The Log: What every software engineer should know about real-time data's unifying abstraction](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying-abstraction) — Jay Kreps, 2013
- [Apache Kafka Introduction — core concepts](https://kafka.apache.org/intro)
- [Apache Kafka Design — Persistence](https://kafka.apache.org/documentation/#design_persistence) — why sequential I/O and append-only logs make Kafka fast
- [Kafka documentation — Replication](https://kafka.apache.org/documentation/#replication) — ISR, high watermark, leader election
- [Kafka documentation — KRaft overview](https://kafka.apache.org/documentation/#kraft) — controller quorum, combined vs dedicated mode

---

## 7. Hands-on in this project

After `./build-deploy.sh` (see root `README.md`) the brokers are up. Inspect the real cluster:

```bash
# List all topics
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list

# Describe the "messages" topic: partitions, replicas, leaders, ISR
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --describe --topic messages
```

Expected output shape:

```
Topic: messages     PartitionCount: 3     ReplicationFactor: 3
Topic: messages     Partition: 0    Leader: 2    Replicas: 2,3,1    Isr: 2,3,1
Topic: messages     Partition: 1    Leader: 3    Replicas: 3,1,2    Isr: 3,1,2
Topic: messages     Partition: 2    Leader: 1    Replicas: 1,2,3    Isr: 1,2,3
```

Note the **leaders are spread across the 3 brokers** and every partition's `Isr` has all 3 replicas — that's replication factor 3 working. When you POST messages and watch the UI's partition column, you are watching this log in action.

**Observation to make:** kill one broker (`docker compose stop kafka-3`) and re-describe the topic. The `Isr` list shrinks, leaders re-elect, and the app keeps producing/consuming. Then `docker compose start kafka-3` and watch `Isr` recover. This is the entire point of RF=3.

---

**Next:** [02 — docker-compose KRaft cluster, env-var by env-var](02-docker-compose-kraft-cluster.md)
