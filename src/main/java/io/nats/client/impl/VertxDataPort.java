package io.nats.client.impl;

import io.nats.client.Options;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetSocket;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class VertxDataPort implements DataPort{
    private NatsConnection connection;
    private String host;
    private int port;

    private final  Vertx vertx;

    private NetClient client;

    private BlockingQueue<Buffer> inputQueue = new ArrayBlockingQueue<>(10);

    private final AtomicReference<NetSocket> socket = new AtomicReference<>();


    public VertxDataPort() {
        vertx = Vertx.vertx();
    }

    public VertxDataPort(final  Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public void connect(final String serverURI, final NatsConnection conn,
                        final long timeoutNanos) throws IOException {

        try {

            this.connection = conn;

            final Options options = this.connection.getOptions();
            long timeout = timeoutNanos / 1_000_000; // convert to millis
            URI uri = null;
            uri = options.createURIForServer(serverURI);

            this.host = uri.getHost();
            this.port = uri.getPort();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        client = vertx.createNetClient();

        client.connect(port, host, event -> {
                    if (event.failed()) {
                        System.out.println("FAILED TO CONNECT");
                        event.cause().printStackTrace();
                    } else {
                        final NetSocket netSocket = event.result();
                        this.socket.set(netSocket);

                        netSocket.handler(buffer -> {
                            try {
                                inputQueue.put(buffer);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        });
                    }
                }
        );

    }

    @Override
    public void upgradeToSecure() throws IOException {

    }

    @Override
    public int read(byte[] dst, int off, int len) throws IOException {
        try {
            final Buffer buffer = inputQueue.poll(50, TimeUnit.MILLISECONDS);
            buffer.getBytes(0, len, dst, off);
            return len;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(byte[] src, int toWrite) throws IOException {

        if (src.length == toWrite) {
            this.socket.get().write(Buffer.buffer(src));
        } else {
            this.socket.get().write(Buffer.buffer( Unpooled.wrappedBuffer(ByteBuffer.wrap(src, 0, toWrite))));
        }
    }

    @Override
    public void shutdownInput() throws IOException {
        Future<Void> close = this.client.close();
        close.result();
    }

    @Override
    public void close() throws IOException {
        Future<Void> close = this.client.close();
        close.result();
    }

    @Override
    public void flush() throws IOException {
    }
}
