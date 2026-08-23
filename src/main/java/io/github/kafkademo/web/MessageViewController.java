package io.github.kafkademo.web;

import io.github.kafkademo.consumer.DltStore;
import io.github.kafkademo.consumer.MessageStore;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * Thymeleaf page: renders the current message + DLT lists on load; the browser then
 * subscribes to the SSE stream for live updates (see docs/PLAN and index.html).
 */
@Controller
public class MessageViewController {

    private final MessageStore store;
    private final DltStore dltStore;

    public MessageViewController(MessageStore store, DltStore dltStore) {
        this.store = store;
        this.dltStore = dltStore;
    }

    @GetMapping("/")
    public String index(Model model) {
        model.addAttribute("messages", store.getMessages());
        model.addAttribute("dltMessages", dltStore.getMessages());
        return "index";
    }
}
