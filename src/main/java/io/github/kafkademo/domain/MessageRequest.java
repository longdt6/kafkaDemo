package io.github.kafkademo.domain;

/** Payload of POST /api/messages. */
public record MessageRequest(String content) {}
