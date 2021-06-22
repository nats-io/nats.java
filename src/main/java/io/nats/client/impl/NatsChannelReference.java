package io.nats.client.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Wraps a NatsChannel so the reference can be modified at a later point in time.
 */
public class NatsChannelReference implements NatsChannel {
    private AtomicReference<NatsChannel> ref;

    public NatsChannelReference(NatsChannel natsChannel) {
        this.ref = new AtomicReference<>(natsChannel);
    }

    @Override
    public boolean isOpen() {
        return ref.get().isOpen();
    }

    @Override
    public void close() throws IOException {
        ref.get().close();
    }

    @Override
    public boolean isSecure() {
        return ref.get().isSecure();
    }

    @Override
    public void shutdownInput() throws IOException {
        ref.get().shutdownInput();
    }

    public void set(NatsChannel natsChannel) {
        this.ref.set(natsChannel);
    }

    public NatsChannel get() {
        return ref.get();
    }

    @Override
    public <A> void read(ByteBuffer dst, A attachment, CompletionHandler<Integer, ? super A> handler) {
        ref.get().read(dst, attachment, handler);
    }

    @Override
    public Future<Integer> read(ByteBuffer dst) {
        return ref.get().read(dst);
    }

    @Override
    public <A> void write(ByteBuffer src, A attachment, CompletionHandler<Integer, ? super A> handler) {
        ref.get().write(src, attachment, handler);
    }

    @Override
    public Future<Integer> write(ByteBuffer src) {
        return ref.get().write(src);
    }
}
