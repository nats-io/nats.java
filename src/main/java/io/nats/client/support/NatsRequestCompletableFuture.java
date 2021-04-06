package io.nats.client.support;

import io.nats.client.Message;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class NatsRequestCompletableFuture extends CompletableFuture<Message> {
    private static final long SAFE_TO_EXPIRE = Duration.ofMinutes(10).toMillis();
    private static final long ENSURE_BEYOND_TIMEOUT = 100; // millis

    private final boolean cancelOn503;
    private final long statusIdWhenCreated;
    private final long expires;

    public NatsRequestCompletableFuture(boolean cancelOn503, Duration timoutIfKnown, long statusId) {
        this.cancelOn503 = cancelOn503;
        if (timoutIfKnown == null) {
            this.expires = System.currentTimeMillis() + SAFE_TO_EXPIRE;
        }
        else {
            this.expires = System.currentTimeMillis() + timoutIfKnown.toMillis() + ENSURE_BEYOND_TIMEOUT; // 100 for just a little more than
        }
        statusIdWhenCreated = statusId;
    }

    public boolean isCancelOn503() {
        return cancelOn503;
    }

    public boolean isExpired(long statusId) {
        return System.currentTimeMillis() > expires ||
                statusId != this.statusIdWhenCreated;
    }
}
