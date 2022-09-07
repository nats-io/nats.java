package io.nats.client.impl;

import io.nats.client.Options;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetSocket;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ArrayBlockingQueue;
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
            final URI uri = options.createURIForServer(serverURI);
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
            final Buffer buffer = inputQueue.poll(30, TimeUnit.SECONDS);
            if (buffer == null) {
                return  -1;
            }
            final int length = Math.min(buffer.length(), len);
            buffer.getBytes(0, length, dst, off);
            return length;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(byte[] src, int length) throws IOException {
        if (src.length == length) {
            this.socket.get().write(Buffer.buffer(src));
        } else {
            Buffer buffer = Buffer.buffer();
            buffer.appendBytes(src, 0, length);
            this.socket.get().write(buffer);
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
