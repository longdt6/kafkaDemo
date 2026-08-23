package io.github.kafkademo.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Consumer-side idempotency (docs/07 §4): remembers message ids that were processed
 * successfully so a re-delivered record (producer retry, crash between process & commit,
 * offset reset) is skipped instead of processed twice.
 *
 * Bounded in-memory set — fine for a demo; a real system would use a DB unique constraint
 * or a Redis set. When the cap is hit we clear and log (honest demo tradeoff).
 */
@Component
@Slf4j
public class DedupStore {

    private static final int MAX_IDS = 10_000;
    private final Set<String> processedIds = ConcurrentHashMap.newKeySet();

    public boolean isProcessed(String id) {
        return processedIds.contains(id);
    }

    public void markProcessed(String id) {
        if (processedIds.size() >= MAX_IDS) {
            log.warn("Dedup set full ({}), clearing to avoid unbounded memory", MAX_IDS);
            processedIds.clear();
        }
        processedIds.add(id);
    }
}
