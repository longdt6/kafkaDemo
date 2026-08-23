package io.github.kafkademo.feed;

import io.github.kafkademo.domain.DltMessage;
import io.github.kafkademo.domain.Message;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Server-Sent Events broadcast hub. Each browser connects via
 * GET /api/messages/stream and gets an emitter; every consumed message is pushed to
 * all open emitters (event name "message"), and every dead-lettered record as event "dlt".
 * Emitters clean themselves up on completion/timeout.
 */
@Component
public class MessageFeed {

    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    public SseEmitter subscribe() {
        // 60s idle timeout; the browser's EventSource auto-reconnects, which returns a fresh emitter.
        SseEmitter emitter = new SseEmitter(60_000L);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitters.add(emitter);
        return emitter;
    }

    public void broadcast(Message message) {
        send("message", message);
    }

    public void broadcastDlt(DltMessage message) {
        send("dlt", message);
    }

    private void send(String eventName, Object data) {
        emitters.forEach(emitter -> {
            try {
                emitter.send(SseEmitter.event().name(eventName).data(data));
            } catch (Exception e) {
                emitters.remove(emitter);
            }
        });
    }
}
