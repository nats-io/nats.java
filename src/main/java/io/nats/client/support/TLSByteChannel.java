package io.nats.client.support;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousByteChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import javax.net.ssl.SSLEngine;

/**
 * TODO: Implement this class based on: https://github.com/nats-io/nats.java/blob/28b4f3563ade5082a2068f43f4264523dec7200e/src/main/java/io/nats/client/support/TLSByteChannel.java
 */
public class TLSByteChannel implements AsynchronousByteChannel {

    public TLSByteChannel(AsynchronousByteChannel wrap, SSLEngine engine) throws IOException {
        throw new UnsupportedOperationException();
    }

    public CompletableFuture<Void> handshake() {
        throw new UnsupportedOperationException();
    }

    public void shutdownInput() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isOpen() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A> void read(ByteBuffer dst, A attachment, CompletionHandler<Integer, ? super A> handler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<Integer> read(ByteBuffer dst) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <A> void write(ByteBuffer src, A attachment, CompletionHandler<Integer, ? super A> handler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<Integer> write(ByteBuffer src) {
        throw new UnsupportedOperationException();
    }

}
