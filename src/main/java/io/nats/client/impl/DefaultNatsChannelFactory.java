package io.nats.client.impl;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CompletableFuture;

import io.nats.client.Options;

public class DefaultNatsChannelFactory implements NatsChannelFactory {
    public static final NatsChannelFactory INSTANCE = new DefaultNatsChannelFactory();

    @Override
    public CompletableFuture<NatsChannel> connect(
        URI serverURI,
        Options options) throws IOException
    {
        return SocketNatsChannel.connect(serverURI);
    }
}
