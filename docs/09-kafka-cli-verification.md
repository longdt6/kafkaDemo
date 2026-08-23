# 09 — The Kafka CLI: verifying everything from the terminal

> The CLI is the ground truth. The UI shows you what *this app* sees; the CLI shows you what the **broker** actually has. Everything here runs against the `apache/kafka` image inside the compose network, so paths are `/opt/kafka/bin/…` and bootstrap is the INTERNAL listener `localhost:19092` *inside a container*.

Because each command lives in a container, the pattern is always:

```
docker exec <broker> /opt/kafka/bin/<tool>.sh --bootstrap-server localhost:19092 <args>
```

Define a helper in your shell to shorten it (Kafka 3.9 removed the old `--zookeeper` path, so `--bootstrap-server` is *the* way):

```bash
k() { docker exec kafka-1 /opt/kafka/bin/"$1" --bootstrap-server localhost:19092 "${@:2}"; }
# usage:  k kafka-topics.sh --list
```

---

## 1. `kafka-topics.sh` — topics, partitions, replicas

```bash
# List all topics
k kafka-topics.sh --list
#   __consumer_offsets
#   messages
#   test-single   (only if you created it in 08)

# Describe one topic: partition count, RF, leaders, ISR
k kafka-topics.sh --describe --topic messages
#   Topic: messages  PartitionCount: 3  ReplicationFactor: 3
#   Topic: messages  Partition: 0  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
#   ...

# Create / alter (for experiments)
k kafka-topics.sh --create --topic demo-2 --partitions 2 --replication-factor 2
k kafka-topics.sh --alter  --topic demo-2 --partitions 4     # can ONLY grow
k kafka-topics.sh --delete --topic demo-2
```

---

## 2. `kafka-console-producer.sh` — write from the terminal

Proves the broker accepts writes *independent of your app*. Since Phase 3 the consumer expects a **JSON `MessagePayload`** value, so use valid JSON (a plain string is still *stored* by the broker — it just goes straight to the DLT when the consumer can't parse it, see `07`):

```bash
# Interactive: type JSON lines, Ctrl-D to finish
k kafka-console-producer.sh --topic messages
#   >{"id":"cli-1","content":"hello from console","sentAt":1787464606000}
#   >{"id":"cli-2","content":"another line","sentAt":1787464607000}

# Non-interactive
echo '{"id":"cli-3","content":"hello from shell","sentAt":1787464608000}' | \
  k kafka-console-producer.sh --topic messages

# With an explicit integer key (must match how you'll read it back)
echo '1:{"id":"cli-4","content":"with key","sentAt":1787464609000}' | \
  k kafka-console-producer.sh --topic messages \
  --property parse.key=true --property key.separator=:
```

The `id` field matters: the consumer dedups on it (`07` §4). Re-send the same `id` and the second copy is logged as *"Duplicate skipped"* and never reaches the UI.

---

## 3. `kafka-console-consumer.sh` — read from the terminal

The most useful tool for *seeing* what's really in the topic:

```bash
# Follow new messages only (like `tail -f`)
k kafka-console-consumer.sh --topic messages

# Replay ALL history (the "replayable log" in action — see 01/04)
k kafka-console-consumer.sh --topic messages --from-beginning

# Show key + partition + timestamp with each value
k kafka-console-consumer.sh --topic messages --from-beginning \
  --property print.key=true --property print.partition=true \
  --property print.timestamp=true

# Read exactly N messages then exit
k kafka-console-consumer.sh --topic messages --max-messages 5
```

Note: a console consumer with no `--group` uses a fresh random group each time → `--from-beginning` replays everything. With a `--group`, it behaves like the app and only gets new messages on the second run (see `04`).

---

## 4. `kafka-consumer-groups.sh` — the operational heart

Lag and offset inspection — the single most useful production command:

```bash
# All groups
k kafka-consumer-groups.sh --list

# Per-partition offsets + lag for the demo's group
k kafka-consumer-groups.sh --describe --group kafka-demo-group
#   GROUP             TOPIC     PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  ...
#   kafka-demo-group  messages  0          12              12              0

# Reset a group's offsets (the "replay from start" trick from 04)
k kafka-consumer-groups.sh --group kafka-demo-group \
  --reset-offsets --to-earliest --execute

# Delete a group entirely (so auto-offset-reset applies again)
k kafka-consumer-groups.sh --delete --group kafka-demo-group
```

**Read the `LAG` column.** 0 = consumer caught up; growing = a consumer is stuck or down. This is your health check.

---

## 5. `kafka-configs.sh` — dynamic configs (per-topic)

```bash
# Show the live effective config of a topic
k kafka-configs.sh --describe --entity-type topics --entity-name messages

# Add a dynamic topic config (e.g. retention, min.insync.replicas — see 08)
k kafka-configs.sh --alter --entity-type topics --entity-name messages \
  --add-config retention.ms=3600000,min.insync.replicas=2

# Remove one
k kafka-configs.sh --alter --entity-type topics --entity-name messages \
  --delete-config retention.ms
```

This is how you set `min.insync.replicas` *per topic* without editing broker config — useful for the `08` experiment.

---

## 6. `kafka-broker-api-versions.sh` — is a broker alive?

The exact command our compose healthcheck uses. Returns OK if the broker responds over the listener:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:19092 | head -5
```

If this hangs or errors, the broker isn't ready — check `docker compose ps` and logs *before* debugging the app.

---

## 7. Full verification script (run after `./build-deploy.sh`)

```bash
set -e
k() { docker exec kafka-1 /opt/kafka/bin/"$1" --bootstrap-server localhost:19092 "${@:2}"; }

echo "== brokers healthy? =="
docker compose ps
echo
echo "== topic layout =="
k kafka-topics.sh --describe --topic messages
echo
echo "== round-trip via CLI =="
echo "cli-probe $(date +%s)" | k kafka-console-producer.sh --topic messages
k kafka-console-consumer.sh --topic messages --from-beginning --max-messages 1 \
  --property print.partition=true --property print.key=true
echo
echo "== round-trip via the app's API =="
curl -s -X POST http://localhost:8080/api/messages \
  -H 'Content-Type: application/json' -d '{"content":"api-probe"}'
echo
echo "== consumer group lag (should settle to 0) =="
sleep 2
k kafka-consumer-groups.sh --describe --group kafka-demo-group
```

---

## 8. Official references

- [Kafka documentation — Quick Start (CLI tour)](https://kafka.apache.org/quickstart)
- `kafka-topics.sh` / `kafka-console-producer.sh` / `kafka-console-consumer.sh` / `kafka-consumer-groups.sh` / `kafka-configs.sh` — all under [Apache Kafka docs](https://kafka.apache.org/documentation/)
- The course repo's `SetUpKafkaDocker.md` / `SetUpKafka3.md` — the same toolkit from a different setup (cp-kafka + ZooKeeper era)

---

**End of the core series.** If you came from the README: you now have the vocabulary to read the app's source (`producer/`, `consumer/`, `config/`) and understand exactly which doc explains each knob.
