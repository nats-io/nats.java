package io.nats.client.impl;

import java.util.Arrays;

import io.nats.client.Options;
import io.nats.client.channels.NatsChannelFactory;
import io.nats.client.channels.SocketNatsChannel;
import io.nats.client.channels.TLSNatsChannel;

public interface DefaultNatsChannelFactory {
    public static NatsChannelFactory.Chain create(Options options) {
        return NatsChannelFactory.buildChain(
            Arrays.asList(
                TLSNatsChannel.factory(options::createSSLEngine)),
            SocketNatsChannel.factory());
    }
}
