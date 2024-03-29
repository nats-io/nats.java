package io.nats.client.support;

import io.nats.client.JetStreamStatusException;
import io.nats.client.Message;

class ReportActionHandler extends ReplyActionHandler {
    @Override
    void handleReply(NatsRequestCompletableFuture future, Message msg) {
        future.completeExceptionally(new JetStreamStatusException(msg.getStatus()));
    }
}