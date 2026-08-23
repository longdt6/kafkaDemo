# 02 — docker-compose: a 3-node KRaft Kafka cluster, env-var by env-var

> This is the fiddliest part of the whole project. Every environment variable is explained below so that, when something breaks, you can reason about *which knob* controls it instead of guessing.

Full file: `docker-compose.yml` at the project root. All three Kafka services are identical except for ids, ports, and hostnames — so we explain **one node** and call out what changes on the others.

---

## 1. The shape: one compose file, four services

```
kafka-1, kafka-2, kafka-3   →  apache/kafka:3.9.2 brokers (combined KRaft mode)
app                         →  our Spring Boot image (built from this repo)
```

The `app` service is in the **same** compose network as the brokers, so it can reach them by their **service hostnames** (`kafka-1:19092`, …) over the INTERNAL listener. There are two client "views" of the cluster in play at once:

| Who connects | Listener | Address they use |
|---|---|---|
| The Spring Boot `app` container | **INTERNAL** | `kafka-1:19092, kafka-2:19093, kafka-3:19094` |
| CLI tools / the `local` profile on your Mac | **EXTERNAL** | `localhost:9092, localhost:9093, localhost:9094` |

This split — two listeners with different advertised addresses — is the classic Kafka-in-Docker trap. The compose file exists to make both work simultaneously.

---

## 2. Node 1 explained line by line

```yaml
kafka-1:
  image: apache/kafka:3.9.2
  container_name: kafka-1
  hostname: kafka-1
  ports:
    - "9092:9092"        # EXTERNAL listener → the host (CLI, local profile)
```

**Why `apache/kafka` and not `cp-kafka`?** The official image is the modern, ZooKeeper-free, KRaft-based one. It's maintained in the Kafka repo itself. `cp-kafka` (Confluent) is how older tutorials — including the course's `docker-compose-multi-broker.yml` — set things up, but that path requires a ZooKeeper container and is legacy as of Kafka 4.0.

### KRaft cluster identity

```yaml
  environment:
    KAFKA_NODE_ID: 1
    KAFKA_PROCESS_ROLES: broker,controller
```

- **`KAFKA_NODE_ID`** — this node's unique id in the cluster (1, 2, or 3).
- **`KAFKA_PROCESS_ROLES: broker,controller`** — *combined mode*: this process is both a data broker and a metadata controller. The 3 nodes together form a 3-member controller quorum. (Alternative: dedicated controllers separate from brokers — not needed here.)

```yaml
    KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:9093,2@kafka-2:9094,3@kafka-3:9095
```

- **`KAFKA_CONTROLLER_QUORUM_VOTERS`** — the static list of controller quorum members, format `nodeId@host:controllerListenerPort`. **Identical on all three nodes.** Note the controller ports are **9093/9094/9095** — deliberately different from the EXTERNAL ports 9092/9093/9094 so the controller listener and the host-mapped listener never collide inside a container.

### Listeners — bind vs advertise (the trap)

```yaml
    KAFKA_LISTENERS: INTERNAL://0.0.0.0:19092,CONTROLLER://0.0.0.0:9093,EXTERNAL://0.0.0.0:9092
    KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka-1:19092,EXTERNAL://localhost:9092
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT
    KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
    KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
```

There are **three different concepts** here and conflating them causes 90% of Kafka-in-Docker failures:

| Concept | What it is |
|---|---|
| **`KAFKA_LISTENERS`** | The ports the process **binds** (`0.0.0.0` = all interfaces). The names (`INTERNAL`, `CONTROLLER`, `EXTERNAL`) are arbitrary labels. |
| **`KAFKA_ADVERTISED_LISTENERS`** | What clients are **told to reconnect to**. The broker sends this back after the initial handshake. If it doesn't match what the client can actually reach, the client connects for the *bootstrap* then fails on *every subsequent request*. |
| **`KAFKA_LISTENER_SECURITY_PROTOCOL_MAP`** | Which security protocol each named listener speaks. **Every name in `LISTENERS` must appear here.** |

Why the asymmetry between `INTERNAL` and `EXTERNAL` advertised values:
- `INTERNAL://kafka-1:19092` — the dockerized `app` resolves `kafka-1` via compose DNS. This is correct **inside** the compose network.
- `EXTERNAL://localhost:9092` — tools on your Mac resolve `localhost:9092` via the published port. This is correct **outside** the network.

```yaml
    KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
    KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
```

- Brokers talk to each other over **INTERNAL**; the controller quorum talks over **CONTROLLER**. Both names must exist in `KAFKA_LISTENERS`.

### The one env var the image *requires*

```yaml
    CLUSTER_ID: 5L6g3nShT-eMCtK--X86sw
```

- **`CLUSTER_ID`** — a base64-encoded 16-byte ID identifying the cluster's metadata log. The official image's entrypoint **throws at startup if this is missing** (it runs `kafka-storage.sh format` with it on every start; formatting is idempotent). **The same value must be on all three nodes.** Ours is a static random-looking string — any valid base64 value works.

### Production hygiene for a 3-node cluster

```yaml
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
    KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
```

- `__consumer_offsets` (the internal topic that stores consumer group offsets) and the transaction state log are **internal** topics. Their RF must be ≤ the number of brokers — with 3 brokers we set RF 3 so no internal data is lost when a node dies.
- `GROUP_INITIAL_REBALANCE_DELAY_MS: 0` — removes the default 3s delay before a new consumer group gets its first assignment. Your first message appears ~instantly instead of after 3 seconds. This is a *demo* convenience; the default exists to reduce churn under real load.

### Healthcheck + storage

```yaml
  healthcheck:
    test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:19092 >/dev/null 2>&1"]
    interval: 10s
    timeout: 10s
    retries: 30
    start_period: 20s
  volumes:
    - kafka-1-data:/tmp/kraft-combined-logs
```

- The healthcheck asks the broker's API over the **INTERNAL** listener. `app` has `depends_on: condition: service_healthy` on all three brokers, so the app starts only once the whole cluster is actually ready — otherwise the app's `KafkaAdmin` would fail to create the `messages` topic with RF 3.
- `/tmp/kraft-combined-logs` is the image's default `log.dirs` (writable by the image's non-root `appuser`). A **named volume** per broker keeps data across `docker compose down` — delete a volume to reset the cluster.

---

## 3. What changes on nodes 2 and 3

Only four things, all mechanical:

| | kafka-1 | kafka-2 | kafka-3 |
|---|---|---|---|
| `KAFKA_NODE_ID` | 1 | 2 | 3 |
| INTERNAL listener | `0.0.0.0:19092` | `0.0.0.0:19093` | `0.0.0.0:19094` |
| INTERNAL advertised | `kafka-1:19092` | `kafka-2:19093` | `kafka-3:19094` |
| EXTERNAL listener | `0.0.0.0:9092` | `0.0.0.0:9093` | `0.0.0.0:9094` |
| EXTERNAL advertised | `localhost:9092` | `localhost:9093` | `localhost:9094` |
| CONTROLLER listener | `0.0.0.0:9093` | `0.0.0.0:9094` | `0.0.0.0:9095` |
| published port | `9092:9092` | `9093:9093` | `9094:9094` |
| volume | `kafka-1-data` | `kafka-2-data` | `kafka-3-data` |

`KAFKA_CONTROLLER_QUORUM_VOTERS`, `CLUSTER_ID`, roles, security map, inter-broker listener, and the replication-factor env vars are **identical** on all three.

---

## 4. The `app` service

```yaml
  app:
    build: .
    container_name: kafka-demo-app
    ports: ["8080:8080"]
    environment:
      SPRING_PROFILES_ACTIVE: default
    depends_on:
      kafka-1: { condition: service_healthy }
      kafka-2: { condition: service_healthy }
      kafka-3: { condition: service_healthy }
    restart: on-failure
```

- `SPRING_PROFILES_ACTIVE: default` activates `application.yml`, whose `bootstrap-servers` points at the **INTERNAL** listeners (`kafka-1:19092`, …). **Never use the `local` profile inside the container** — `localhost` there is the container itself, not your Mac.
- `restart: on-failure` gives the app a chance to recover if Kafka hiccups during startup.
- `depends_on: condition: service_healthy` is Compose v2 syntax for "wait until healthy" (not just "container started").

---

## 5. Common errors table

| Symptom | Likely cause | Fix |
|---|---|---|
| Container exits instantly, log shows *"CLUSTER_ID not set"* | `CLUSTER_ID` env var missing | Add the same `CLUSTER_ID` to all 3 nodes |
| Client logs *"Connection to node -1 could not be established. Broker may not be available."* | Advertised listener unreachable | `docker exec kafka-1 cat /etc/hosts`… check `KAFKA_ADVERTISED_LISTENERS` hostname resolves for the client |
| App connects but every request times out | Advertised INTERNAL hostname is `localhost`/wrong | Advertise the **service hostname** (`kafka-1:19092`) for INTERNAL |
| *"Configuration property does not exist: 'zookeeper.connect'"* or use of `KAFKA_BROKER_ID` | Legacy ZooKeeper-era env var copied from an old tutorial | Remove it; use `KAFKA_NODE_ID` + `KAFKA_PROCESS_ROLES` |
| Topic created with 1 partition / 1 replica despite RF 3 | Broker-side `auto.create.topics.enable` created it before the app's admin ran, or fewer than 3 brokers up | Wait for 3 healthy brokers before starting the app; the `NewTopic` bean (see `05`) re-creates with RF 3 |
| *"Error creating topic … replication factor: 3 larger than available brokers: 2"* | One broker not up when admin created the topic | `docker compose ps` — all 3 healthy; then delete topic + restart app |
| Controller and EXTERNAL ports collide (`Address already in use`) | Reused 9092/9093/9094 for the controller listener | Controllers use 9093/9094/9095 (see table above) |
| Host CLI connects to `localhost:9092` but cluster still "warming up" | Kafka not fully initialized | Wait for `docker compose ps` healthy, then `kafka-topics.sh --list` |

---

## 6. Official references

- [Apache Kafka — KRaft (KIP-500) overview](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum)
- [apache/kafka image docs (Docker Hub)](https://hub.docker.com/r/apache/kafka) — env var conventions, KRaft examples
- [Kafka configuration — listeners](https://kafka.apache.org/documentation/#brokerconfigs_listeners) and [advertised.listeners](https://kafka.apache.org/documentation/#brokerconfigs_advertised.listeners)
- [Kafka configuration — controller.quorum.voters](https://kafka.apache.org/documentation/#controllerconfigs_controller.quorum.voters)
- The course repo's old ZooKeeper setup for *contrast*: `docker-compose-multi-broker.yml` in the `kafka-for-developers-using-spring-boot-v2` folder

---

## 7. Hands-on

Isolate the fiddly part before touching the app:

```bash
# 1. Brokers only
docker compose up -d kafka-1 kafka-2 kafka-3
docker compose ps          # all three HEALTHY

# 2. Sanity check the cluster end-to-end
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list

# 3. Break it on purpose: stop one node, watch ISR shrink (see 01)
docker compose stop kafka-3
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --describe --topic messages
docker compose start kafka-3

# 4. Full stack later
./build-deploy.sh
```

**Next:** [03 — Producer deep-dive](03-producer-deep-dive.md)
