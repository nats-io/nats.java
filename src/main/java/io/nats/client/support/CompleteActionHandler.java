package io.nats.client.support;

import io.nats.client.Message;

class CompleteActionHandler extends ReplyActionHandler {
    @Override
    void handleReply(NatsRequestCompletableFuture future, Message msg) {
        future.complete(msg);
    }
}
