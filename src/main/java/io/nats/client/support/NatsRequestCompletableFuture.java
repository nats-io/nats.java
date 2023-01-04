package io.nats.client.support;

import io.nats.client.Message;
import io.nats.client.Options;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

public class NatsRequestCompletableFuture extends CompletableFuture<Message> {
    private static final long DEFAULT_TIMEOUT = Options.DEFAULT_REQUEST_CLEANUP_INTERVAL.toMillis(); // currently 5 seconds

    private final boolean cancelOn503;
    private final long timeOutAfter;
    private boolean wasCancelledClosing;
    private boolean wasCancelledTimedOut;

    public NatsRequestCompletableFuture(boolean cancelOn503, Duration timeout) {
        this.cancelOn503 = cancelOn503;
        timeOutAfter = System.currentTimeMillis() + 10 + (timeout == null ? DEFAULT_TIMEOUT : timeout.toMillis());
        // 10 extra millis allows for communication time, probably more than needed but...
    }

    public void cancelClosing() {
        wasCancelledClosing = true;
        completeExceptionally(new CancellationException("Future cancelled, connection closing."));
    }

    public void cancelTimedOut() {
        wasCancelledTimedOut = true;
        completeExceptionally(new CancellationException("Future cancelled, response not registered in time, likely due to server disconnect."));
    }

    public boolean isCancelOn503() {
        return cancelOn503;
    }

    public boolean hasExceededTimeout() {
        return System.currentTimeMillis() > timeOutAfter;
    }

    public boolean wasCancelledClosing() {
        return wasCancelledClosing;
    }

    public boolean wasCancelledTimedOut() {
        return wasCancelledTimedOut;
    }
}
