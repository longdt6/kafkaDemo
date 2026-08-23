package io.github.kafkademo.consumer;

import io.github.kafkademo.domain.DltMessage;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * In-memory, thread-safe store of the last {@value #MAX} dead-lettered records
 * (written by the DLT listener thread, read by the MVC thread rendering the page).
 */
@Component
public class DltStore {

    private static final int MAX = 100;
    private final List<DltMessage> messages = new CopyOnWriteArrayList<>();

    public void add(DltMessage message) {
        messages.add(message);
        if (messages.size() > MAX) {
            messages.remove(0);
        }
    }

    /** Defensive snapshot so callers can't mutate the store. */
    public List<DltMessage> getMessages() {
        return List.copyOf(messages);
    }
}
