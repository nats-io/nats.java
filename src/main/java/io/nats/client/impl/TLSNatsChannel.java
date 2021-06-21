package io.nats.client.impl;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import javax.net.ssl.SSLEngine;

import io.nats.client.Options;
import io.nats.client.support.TLSByteChannel;

public class TLSNatsChannel implements NatsChannel {
    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

    private NatsChannel wrap;
    private TLSByteChannel byteChannel;

    public static NatsChannel wrap(
        NatsChannel natsChannel,
        URI uri,
        Options options,
        Consumer<Exception> handleCommunicationIssue,
        Duration timeout)
        throws IOException
    {
        return new TLSNatsChannel(natsChannel, uri, options, handleCommunicationIssue, timeout);
    }

    private TLSNatsChannel(
        NatsChannel wrap,
        URI uri,
        Options options,
        Consumer<Exception> handleCommunicationIssue,
        Duration timeout)
        throws IOException
    {
        this.wrap = wrap;
        SSLEngine engine = options.getSslContext().createSSLEngine(uri.getHost(), uri.getPort());
        engine.setUseClientMode(true);
        this.byteChannel = new TLSByteChannel(wrap, engine);

        AtomicReference<Exception> handshakeException = new AtomicReference<>();
        Thread handshakeThread = new Thread(() -> {
            try {
                this.byteChannel.write(EMPTY);
            } catch (Exception ex) {
                handshakeException.set(ex);
            }
        });
        try {
            handshakeThread.start();
            handshakeThread.join(timeout.toMillis());
            Exception ex = handshakeException.get();
            if (null != ex) {
                handleCommunicationIssue.accept(ex);
            }
        } catch (InterruptedException ex) {
            handleCommunicationIssue.accept(
                new TimeoutException("Timeout waiting for TLS handshake"));
        }
    }

    @Override
    public boolean isSecure() {
        return true;
    }

    @Override
    public void shutdownInput() throws IOException {
        // cannot call shutdownInput on sslSocket
    }

    @Override
    public void close() throws IOException {
        byteChannel.close(); // auto closes the underlying socket
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return byteChannel.read(dst);
    }

    @Override
    public boolean isOpen() {
        return wrap.isOpen();
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return byteChannel.write(src);
    }

    @Override
    public void flushOutput() throws IOException {
        wrap.flushOutput();
    }
    
}
