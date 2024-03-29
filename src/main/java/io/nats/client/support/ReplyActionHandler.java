package io.nats.client.support;

import io.nats.client.Message;

abstract class ReplyActionHandler {
    abstract void handleReply(NatsRequestCompletableFuture future, Message msg);
}

