package io.nats.client.impl;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import javax.net.ssl.SSLEngine;

import io.nats.client.Options;
import io.nats.client.support.TLSByteChannel;

public class TLSNatsChannel implements NatsChannel {
    private TLSByteChannel byteChannel;

    /**
     * Future is completed when the initial handshake completes.
     */
    public static CompletableFuture<NatsChannel> wrap(
        NatsChannel natsChannel,
        URI uri,
        Options options)
        throws IOException
    {
        SSLEngine engine = options.getSslContext().createSSLEngine(uri.getHost(), uri.getPort());
        engine.setUseClientMode(true);
        TLSByteChannel byteChannel = new TLSByteChannel(natsChannel, engine);
        return byteChannel.handshake().thenApply(ignored -> {
            return new TLSNatsChannel(byteChannel);
        });
    }

    private TLSNatsChannel(TLSByteChannel byteChannel) {
        this.byteChannel = byteChannel;
    }

    @Override
    public boolean isSecure() {
        return true;
    }

    @Override
    public void shutdownInput() throws IOException {
        byteChannel.shutdownInput();
    }

    @Override
    public void close() throws IOException {
        byteChannel.close();
    }

    @Override
    public boolean isOpen() {
        return byteChannel.isOpen();
    }

    @Override
    public <A> void read(ByteBuffer dst, A attachment, CompletionHandler<Integer, ? super A> handler) {
        byteChannel.read(dst, attachment, handler);
    }

    @Override
    public Future<Integer> read(ByteBuffer dst) {
        return byteChannel.read(dst);
    }

    @Override
    public <A> void write(ByteBuffer src, A attachment, CompletionHandler<Integer, ? super A> handler) {
        byteChannel.write(src, attachment, handler);
    }

    @Override
    public Future<Integer> write(ByteBuffer src) {
        return byteChannel.write(src);
    }
    
}
