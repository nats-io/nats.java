package io.nats.client.impl;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static java.net.StandardSocketOptions.TCP_NODELAY;
import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.net.StandardSocketOptions.SO_SNDBUF;

/**
 * Simple wrapper around {@link AsynchronousSocketChannel}
 */
public class SocketNatsChannel implements NatsChannel {
    private AsynchronousSocketChannel socket;

    public static CompletableFuture<NatsChannel> connect(
        URI serverURI)
        throws IOException
    {
        try {
            AsynchronousSocketChannel socket = AsynchronousSocketChannel.open();
            socket.setOption(TCP_NODELAY, true);
            socket.setOption(SO_RCVBUF, 2 * 1024 * 1024);
            socket.setOption(SO_SNDBUF, 2 * 1024 * 1024);

            CompletableFuture<NatsChannel> future = new CompletableFuture<NatsChannel>() {
                @Override
                public boolean cancel(boolean mayInterruptIfRunning) {
                    try {
                        socket.close();
                    } catch (IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                    return true;
                }
            };
    
            socket.connect(
                new InetSocketAddress(serverURI.getHost(), serverURI.getPort()),
                null,
                new CompletionHandler<Void, Void>() {
                    @Override
                    public void completed(Void result, Void attachment) {
                        future.complete(new SocketNatsChannel(socket));
                    }

                @Override
                public void failed(Throwable exc, Void attachment) {
                    future.completeExceptionally(exc);
                }

            });
            return future;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    private SocketNatsChannel(AsynchronousSocketChannel socket) {
        this.socket = socket;
    }

    @Override
    public <A> void read(ByteBuffer dst, A attachment, CompletionHandler<Integer, ? super A> handler) {
        socket.read(dst, attachment, handler);
    }

    @Override
    public Future<Integer> read(ByteBuffer dst) {
        return socket.read(dst);
    }

    @Override
    public <A> void write(ByteBuffer src, A attachment, CompletionHandler<Integer, ? super A> handler) {
        socket.write(src, attachment, handler);
    }

    @Override
    public Future<Integer> write(ByteBuffer src) {
        return socket.write(src);
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }

    @Override
    public boolean isOpen() {
        return socket.isOpen();
    }

    @Override
    public boolean isSecure() {
        return false;
    }

    @Override
    public void shutdownInput() throws IOException {
        socket.shutdownInput();
    }
}
