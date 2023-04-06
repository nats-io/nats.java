package io.nats.examples.jetstream.simple;

import io.nats.client.Connection;
import io.nats.client.ErrorListener;
import io.nats.client.JetStreamSubscription;
import io.nats.client.support.Status;

public class SimpleErrorListener implements ErrorListener {
    public Status pullStatusWarning;
    public Status pullStatusError;

    @Override
    public void pullStatusWarning(Connection conn, JetStreamSubscription sub, Status status) {
        pullStatusWarning = status;
    }

    @Override
    public void pullStatusError(Connection conn, JetStreamSubscription sub, Status status) {
        pullStatusError = status;
    }

    public void reset() {
        pullStatusWarning = null;
        pullStatusError = null;
    }
}
