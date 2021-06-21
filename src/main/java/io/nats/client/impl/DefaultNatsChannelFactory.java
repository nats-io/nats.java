package io.nats.client.impl;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.function.Consumer;

import io.nats.client.Options;

public class DefaultNatsChannelFactory implements NatsChannelFactory {
    public static final NatsChannelFactory INSTANCE = new DefaultNatsChannelFactory();

    @Override
    public NatsChannel connect(
        URI serverURI,
        Options options,
        Consumer<Exception> handleCommunicationIssue,
        Duration timeout) throws IOException
    {
        return SocketNatsChannel.connect(serverURI, timeout);
    }
}
