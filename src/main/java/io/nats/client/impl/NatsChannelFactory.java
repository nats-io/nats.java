package io.nats.client.impl;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CompletableFuture;

import io.nats.client.Options;

@FunctionalInterface
public interface NatsChannelFactory {
    /**
     * Create a new NatsChannel for the given serverURI, options, and remaining timeout in nanoseconds.
     */
    public CompletableFuture<NatsChannel> connect(
        URI serverURI,
        Options options) throws IOException;
}
