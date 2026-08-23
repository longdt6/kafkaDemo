package io.github.kafkademo.domain;

import java.util.UUID;

/**
 * A record that exhausted all retries and landed in the dead-letter topic.
 *
 * @param id        the payload id (or "unknown" if the DLT record wasn't valid JSON)
 * @param partition partition of the DLT record
 * @param offset    offset of the DLT record
 * @param content   the payload content (or the raw string if JSON parse failed)
 * @param errorClass FQCN of the exception that finally killed it
 * @param timestamp DLT record timestamp (ms since epoch)
 */
public record DltMessage(String id, int partition, long offset, String content, String errorClass, long timestamp) {

    public DltMessage(int partition, long offset, String content, String errorClass, long timestamp) {
        this(UUID.randomUUID().toString(), partition, offset, content, errorClass, timestamp);
    }
}
