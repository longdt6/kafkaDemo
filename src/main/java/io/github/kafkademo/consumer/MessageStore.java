package io.github.kafkademo.consumer;

import io.github.kafkademo.domain.Message;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * In-memory, thread-safe store of the last {@value #MAX} consumed messages.
 * Written by the Kafka listener thread, read by the MVC thread rendering the page.
 * (A real system would use Redis/a DB — see docs/07 on idempotency & dedup.)
 */
@Component
public class MessageStore {

    private static final int MAX = 100;
    private final List<Message> messages = new CopyOnWriteArrayList<>();

    public void add(Message message) {
        messages.add(message);
        if (messages.size() > MAX) {
            messages.remove(0);
        }
    }

    /** Defensive snapshot so callers can't mutate the store. */
    public List<Message> getMessages() {
        return List.copyOf(messages);
    }
}
