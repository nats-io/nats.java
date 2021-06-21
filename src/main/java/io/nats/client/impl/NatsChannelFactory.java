package io.nats.client.impl;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.function.Consumer;

import io.nats.client.Options;

@FunctionalInterface
public interface NatsChannelFactory {
    /**
     * Create a new NatsChannel for the given serverURI, options, and remaining timeout in nanoseconds.
     */
    public NatsChannel connect(URI serverURI, Options options, Consumer<Exception> handleCommunicationIssue, Duration timeout) throws IOException;
}
