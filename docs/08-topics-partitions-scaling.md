# 08 — Topics, Partitions & Scaling

> Everything in this doc is a decision you make *once* at topic creation, or a lever that bites you later. The `messages` topic has 3 partitions × RF 3 — this doc explains why those two numbers exist and what happens if you change them.

---

## 1. Partitions bound parallelism — on both sides

Partition count is the ceiling on both producer and consumer throughput:

**Consumer side:** a single consumer instance reads one or more whole partitions; **a partition is read by at most one member of a group**. So with 3 partitions, the most parallelism a group gets is **3** (3 consumers, one partition each). Add a 4th consumer → it sits idle. To scale consumption you must *also* add partitions.

```
3 partitions, 5 consumers           3 partitions, 5 consumers
group A:                             (replicate this app 5×) →
  consumer1 ─ p0                      consumer1 ─ p0
  consumer2 ─ p1                      consumer2 ─ p1
  consumer3 ─ p2                      consumer3 ─ p2
  consumer4 ─ (idle)                  consumer4 ─ (idle)
  consumer5 ─ (idle)                  consumer5 ─ (idle)
```

**Producer side:** producers write to different partition leaders in parallel. Few partitions = hot partitions / limited fan-out; many partitions = more brokers involved and more metadata overhead.

**Ordering trade-off:** more partitions spreads keys thinner but also *fragments ordering scope* — order is only guaranteed per (partition). If you need strict global order for a key, that key must always land in the same partition (see `03`, §4).

---

## 2. Replication factor (RF) — the durability number

RF = number of *copies* of each partition, spread across different brokers. `ReplicationFactor: 3` on our `messages` topic means every partition exists on 3 of the 3 brokers.

- **RF 3 ⇒ can lose 2 brokers and still serve** every partition from the surviving ISR member (Kafka can tolerate `RF - 1` broker failures and keep serving).
- **RF 1** (embedded broker / a single broker) — lose that broker, lose the topic. Fine for dev, fatal for production.
- **RF must be ≤ number of brokers.** You literally cannot create RF 3 on a 2-broker cluster (`08` of this project's error table).

Where RF is decided: at **topic creation** (`NewTopic` bean, or CLI `--replication-factor`). You can change it later with `kafka-configs.sh`, but it's a manual operation — get it right up front.

---

## 3. `acks=all` + `min.insync.replicas` — the "not losing committed data" guarantee

`acks=all` (§2 of `03`) means the leader + **all in-sync replicas** ack. That phrase contains the trap: if the ISR is `[leader]` only (2 followers down), `acks=all` ack's after *one* replica. `min.insync.replicas` puts a floor on that:

```yaml
# Broker-level (docker-compose / broker config), typically per-topic:
min.insync.replicas = 2
```

Meaning: the leader **refuses to accept writes unless at least 2 replicas are in sync**. Combined with `acks=all`:

| `acks` | `min.insync.replicas` | Result |
|---|---|---|
| `all` | 1 | write ack'd as soon as 1 replica (leader) has it — weakest "all" |
| `all` | 2 | leader + ≥1 follower must be in-sync; if only 1 in-sync replica remains, **writes fail** (client gets `NotEnoughReplicasException`) — no *silent* loss |
| `all` | `RF` | impossible under normal operation (all replicas would need to be in-sync every time) |

The takeaway: **`acks=all` alone is not a strong guarantee — `min.insync.replicas` is what makes it strong.** Our compose sets `min.insync.replicas` behavior implicitly (RF 3, ISR 3); the explicit number is a production concern. When you later set the topic up "for real", this pair (`acks=all` + `min.insync.replicas=2`) is the standard "never lose committed data" config.

---

## 4. How many partitions should a topic have?

Rules of thumb (they're *heuristics*, not laws):

1. **Throughput target:** pick so a single partition's leader can handle the load. Typical production: 6–30 partitions per topic; thousands is anti-pattern (metadata overhead, rebalance churn).
2. **Max consumer parallelism = partition count.** If you expect N consumers in a group, you need ≥ N partitions.
3. **Future growth:** Kafka **cannot decrease** partitions, and increasing them is a one-way door (per-key ordering changes — `murmur2(key) % newCount` maps old keys to *different* partitions). Choose a number that covers your near-term scale; re-architect, don't renumber.

For this demo, **3 partitions** is chosen because the cluster has 3 brokers — you can *see* leaders spread across all three in `kafka-topics --describe` (RF 3 × 3 brokers = every partition on all nodes). It's a teaching number, not a production one.

---

## 5. "Partitions" in the UI — what you're actually seeing

Every consumed row in the demo's table shows `partition` and `offset`. Those two numbers *are* a Kafka record's identity (`01`, §2). Watch them:

- Send 6 messages → partitions 0,1,2 cycle (the `murmur2` key spread from `03`).
- Each partition's offsets are **independent**: `offset 0` in partition 0 and `offset 0` in partition 1 are *different messages*. The UI rows make the per-partition log visible.

---

## 6. Official references

- [Kafka documentation — Topics and partitions](https://kafka.apache.org/documentation/#intro_topics)
- [Kafka documentation — `min.insync.replicas`](https://kafka.apache.org/documentation/#brokerconfigs_min.insync.replicas)
- [Kafka documentation — `default.replication.factor` / replication](https://kafka.apache.org/documentation/#replication)
- [LinkedIn / Confluent blog — How to choose the number of partitions](https://www.confluent.io/blog/how-to-choose-the-number-of-topics-partitions-kafka-cluster/)

---

## 7. Hands-on in this project

```bash
# 1. Read the topic's live partition/replica layout (RF 3 on 3 brokers)
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 --describe --topic messages

# 2. Show WHY RF matters: stop a broker, produce more messages, re-describe
docker compose stop kafka-3
for i in 7 8 9; do curl -s -X POST http://localhost:8080/api/messages \
  -H 'Content-Type: application/json' -d "{\"content\":\"msg $i\"}"; echo; done
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --describe --topic messages
docker compose start kafka-3
#    → ISR drops to [kafka-1, kafka-2] while kafka-3 is down, recovers to 3 after restart.
#      With RF 3, no partition ever loses its committed data during this.

# 3. (Optional) create a second topic with different partitions and compare --describe output
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --topic test-single --partitions 1 --replication-factor 1
```

---

**Next:** [09 — The Kafka CLI: verifying everything](09-kafka-cli-verification.md)
