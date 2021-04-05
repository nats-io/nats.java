package io.nats.client.support;

import io.nats.client.Message;

import java.util.concurrent.CompletableFuture;

public class NatsRequestCompletableFuture extends CompletableFuture<Message> {
    private boolean regular;
    private long stamp;

    public NatsRequestCompletableFuture(boolean regular) {
        this.regular = regular;
        this.stamp = System.currentTimeMillis();
    }

    public boolean isRegular() {
        return regular;
    }

    public long getStamp() {
        return stamp;
    }

    public boolean isOlderThan(long thisManyMillis) {
        return System.currentTimeMillis() - stamp > thisManyMillis;
    }
}
