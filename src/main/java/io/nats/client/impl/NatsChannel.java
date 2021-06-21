package io.nats.client.impl;

import java.io.IOException;
import java.nio.channels.ByteChannel;

/**
 * Low-level API for establishing a connection. This allows us to support the
 * "decorator" design pattern to support TLS, Websockets, and HTTP Proxy support.
 */
public interface NatsChannel extends ByteChannel {
    /**
     * When performing the NATS INFO/CONNECT handshake, we may need to
     * upgrade to a secure connection, but if this connection is already
     * secured, it should be a no-op.
     */
    boolean isSecure();

    /**
     * Shutdown the reader side of the channel.
     */
    void shutdownInput() throws IOException;

    /**
     * Flush the writer side of the channel.
     */
    void flushOutput() throws IOException;    
}
