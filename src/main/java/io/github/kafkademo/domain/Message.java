package io.github.kafkademo.domain;

import java.util.UUID;

/**
 * A message as the UI sees it.
 *
 * @param id        display UUID (would be the dedup key in a real system — see docs/07)
 * @param partition partition the record landed in (from the broker)
 * @param offset    offset within that partition (from the broker)
 * @param content   the user's message text
 * @param timestamp broker-side record timestamp (ms since epoch)
 */
public record Message(String id, int partition, long offset, String content, long timestamp) {

    public Message(int partition, long offset, String content, long timestamp) {
        this(UUID.randomUUID().toString(), partition, offset, content, timestamp);
    }
}
