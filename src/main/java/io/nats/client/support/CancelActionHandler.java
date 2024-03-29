package io.nats.client.support;

import io.nats.client.Message;

class CancelActionHandler extends ReplyActionHandler {
    @Override
    void handleReply(NatsRequestCompletableFuture future, Message msg) {
        future.cancel(true);
    }
}
