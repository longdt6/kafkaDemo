# 06 — Serialization: bytes, keys, values, and JSON

> Kafka stores **raw bytes**. Keys and values are just byte arrays from the broker's point of view. *You* decide the format on both sides — and the two sides must agree. This doc is about the `Serializer`/`Deserializer` pair and what happens when they disagree.

---

## 1. The broker is format-agnostic

A topic's records are `(key: bytes, value: bytes, timestamp, headers)`. No serializer, no schema, no validation on the broker. Two consequences:

1. **You own the format.** Producer serializes `String`/`Integer`/`Object` → bytes; consumer deserializes bytes back. If they don't match, you get garbage or a crash.
2. **Bytes are what let polyglot consumers exist** — a Python consumer reading the same topic just needs the same format, not your Java classes.

This is why "serialization" is a *contract between producer and consumer*, not a property of the topic.

---

## 2. The serializer/deserializer pair in this project

```yaml
# application.yml
producer:
  key-serializer: org.apache.kafka.common.serialization.IntegerSerializer
  value-serializer: org.apache.kafka.common.serialization.StringSerializer
consumer:
  key-deserializer: org.apache.kafka.common.serialization.IntegerDeserializer
  value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
```

- Key = `Integer` (our incrementing counter) ↔ `IntegerSerializer`/`IntegerDeserializer` (4-byte big-endian int).
- Value = `String` (a JSON payload — see §3) ↔ `StringSerializer`/`StringDeserializer` (UTF-8).

`KafkaTemplate<Integer, String>` and `ConsumerRecord<Integer, String>` generics must match these — the template is typed to the serializers. A mismatch (e.g. template says `String` key but serializer is `IntegerSerializer`) fails at runtime, not compile time.

### The built-in serializers you'll meet

| Class | Format | When |
|---|---|---|
| `StringSerializer` | UTF-8 bytes | text, JSON strings |
| `IntegerSerializer` / `LongSerializer` | big-endian int/long | numeric keys (ordering-friendly) |
| `ByteArraySerializer` | raw bytes | already-serialized payloads |
| `FloatSerializer`/`DoubleSerializer` | IEEE 754 | numeric values |

---

## 3. Serializing real objects: JSON + Jackson

For anything richer than a string you reach for **JSON**. The producer serializes an object with `ObjectMapper` and the consumer deserializes back — the course's `LibraryEvent`/`Book` pattern. **This demo does exactly that**: the value on the wire is a `MessagePayload` (`id`, `content`, `sentAt`) serialized to JSON. The real code in `MessageProducer`:

```java
MessagePayload payload = new MessagePayload(content);          // id = UUID, sentAt = now
String json = objectMapper.writeValueAsString(payload);        // → {"id":...,"content":...,"sentAt":...}
ProducerRecord<Integer, String> record = new ProducerRecord<>(
        topic, null, key, json,
        List.of(new RecordHeader("schema-version", "1".getBytes())));
kafkaTemplate.send(record);                                     // full record: partition, key, value, headers
```

and the consumer parses it back:

```java
MessagePayload payload = objectMapper.readValue(record.value(), MessagePayload.class);
```

**Key points for the JSON pattern:**
- The **value type** is still `String` on the wire (JSON is a string); the `StringSerializer`/`StringDeserializer` pair stays the same. Only your application code (de)serializes objects ↔ JSON.
- Jackson handles Java records natively (it uses the canonical constructor) — no extra config for `MessagePayload`.
- The deserializer class must have a default constructor / work without args (Spring instantiates it) — the standard `Jackson` deserializers do.
- Headers (see §5) are the standard place to put a **content type / schema version** so consumers can distinguish payload formats — this demo's producer attaches a `schema-version: 1` header.

---

## 4. Custom serializers — and why they're usually a trap

Spring lets you write your own `Serializer<T>`/`Deserializer<T>` (implement the interfaces, return `byte[]`). Three reasons to avoid it:

1. **Tight coupling to one producer's format** — a Java-serialized object (`ObjectOutputStream`) is unreadable by any other language. This is the classic "it works but only if everyone is Java" footgun.
2. **Schema drift** — no versioning story. Change a field and old data breaks the deserializer.
3. **You reimplement what Jackson already does.**

**The production answer is a schema registry** (Confluent Schema Registry + Avro/Protobuf with `io.confluent:kafka-avro-serializer`): schema on the record, versions in the registry, cross-language safe. That's the real-world pattern this whole doc points toward — but for a learning demo, JSON is right, and understanding *why* the registry exists is the lesson.

---

## 5. Headers

Records carry a small **header map** (key → byte[] value) alongside key/value. Useful for metadata *without* touching the payload:

```java
// Producer: attach a header (course's buildProducerRecord pattern)
ProducerRecord<Integer, String> record =
    new ProducerRecord<>(topic, null, key, value,
        List.of(new RecordHeader("event-source", "web-ui".getBytes())));
kafkaTemplate.send(record);

// Consumer: read it back
byte[] source = record.headers().lastHeader("event-source").value();
```

Common uses: content type, schema version, origin service, trace/correlation IDs. This demo's producer attaches a **`schema-version: 1`** header to every record (see §3) — a real system would branch on it to deserialize differently per version. The DLT path (see `07`) also relies on headers: the exception info the `@DltHandler` reads (`kafka_exception-cause-fqcn`, …) is carried as record headers on the dead-lettered record.

---

## 6. Deserialization failure is a *poison pill*

If a record's bytes can't be deserialized (wrong type, corrupt data, schema drift), the listener throws. By default the consumer keeps **retrying the same record**, then — with a `DefaultErrorHandler` — sends it to the dead-letter topic and *skips it* (see `07`). Without an error handler, a poison pill can stall your whole listener. This is a real operational concern, not an edge case: **a serialization contract that drifts breaks consumers at runtime.**

---

## 7. Official references

- [Kafka documentation — Message format](https://kafka.apache.org/documentation/#messageformat)
- [Kafka client — `Serialization`](https://docs.oracle.com/javase/8/docs/api/org/apache/kafka/common/serialization/package-summary.html) (`Serializer`/`Deserializer`/`Serde`)
- [Confluent Schema Registry docs](https://docs.confluent.io/platform/current/schema-registry/index.html) — why production systems don't hand-roll serializers
- The course repo's `LibraryEventProducer` (JSON + ObjectMapper) and `Book`/`LibraryEvent` domain classes

---

## 8. Hands-on in this project

```bash
# 1. Prove the wire format: a console consumer shows the raw JSON value + key
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:19092 --topic messages --from-beginning \
  --property print.key=true --property print.value=true
#    → key is an int, value is a JSON string like {"id":"...","content":"...","sentAt":...}

# 2. Send a NON-JSON value (e.g. plain text) — the consumer can't parse it:
echo "not json" | docker exec -i kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:19092 --topic messages
#    It's excluded from retries (JsonProcessingException) and lands straight in the DLT.
#    Check the UI's DLT table, or:
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:19092 --topic messages-dlt --from-beginning
```

**The live experiment is already done:** `MessagePayload` JSON + a `schema-version` header on every record is exactly the step from a "just strings" demo to the course's `LibraryEvent` pattern.

---

**Next:** [07 — Error handling, retries, and the dead-letter topic](07-error-handling-retry-dlt.md)
