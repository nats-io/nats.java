package io.nats.client.impl;

import io.nats.client.Dispatcher;
import io.nats.client.Options;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class VertxDataPort implements DataPort{
    private final boolean ownVertx;
    private NatsConnection connection;
    private String host;
    private int port;

    private final  Vertx vertx;

    private NetClient client;

    private BlockingQueue<Buffer> inputQueue = new ArrayBlockingQueue<>(10);

    private final AtomicReference<NetSocket> socket = new AtomicReference<>();
    private NatsConnectionReader reader;
    private NatsConnectionWriter writer;


    public VertxDataPort() {
        vertx = Vertx.vertx();
        ownVertx=false;
    }

    public VertxDataPort(final  Vertx vertx) {
        this.ownVertx = true;
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
                    connect(event);
                }
        );
        vertx.setTimer(100, event -> doWrite());
        vertx.setTimer(100, event -> handleDispatchers());
    }

    private void connect(AsyncResult<NetSocket> event) {
        if (event.failed()) {

            if (event.cause() instanceof Exception) {
                this.connection.handleCommunicationIssue((Exception) event.cause());
            } else {
                event.cause().printStackTrace();
            }
        } else {
            final NetSocket netSocket = event.result();
            this.socket.set(netSocket);
            netSocket.handler(buffer -> {
                try {
                    inputQueue.put(buffer);
                    if (reader!=null) reader.readNow();
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private void handleDispatchers() {
        if (this.connection.dispatchers.size() == 0) {
            vertx.setTimer(100, event -> handleDispatchers());
            return;
        }
        connection.dispatchers.values().stream().map(m -> (Dispatcher) m).forEach( d -> {
                        if (!d.processNextMessage()) {
                            connection.dispatchers.remove(d.getId());
                        }
                }
        );
        vertx.runOnContext(event -> handleDispatchers());
    }

    private void doWrite() {
        if (writer!=null) {
            int sent = writer.writeMessages();
            if (sent <= 0) {
                vertx.setTimer(50, event -> doWrite());
            } else {
                vertx.runOnContext(event -> doWrite());
            }
        }
    }

    @Override
    public void upgradeToSecure() throws IOException {
        final Options options = this.connection.getOptions();
        final Duration timeout = options.getConnectionTimeout();

        //https://vertx.io/docs/vertx-core/java/#_upgrading_connections_to_ssltls

        if (options.isTLSRequired()) {
            final NetClientOptions clientOptions = new NetClientOptions().setSsl(options.isTLSRequired());
            if (options.getTlsKeystorePath() != null) {
                final String path = options.getTlsKeystorePath();
                if (path.endsWith("jks")) {
                    final JksOptions keyStoreOptions = new JksOptions().setPath(path);
                    if (options.getTlsTruststorePassword() !=null) {
                        keyStoreOptions.setPassword(new String(options.getTlsKeystorePassword()));
                    }
                    clientOptions.setKeyStoreOptions(keyStoreOptions);
                } else if (path.endsWith("pfx")) {
                    final PfxOptions keyStoreOptions = new PfxOptions().setPath(path);
                    if (options.getTlsKeystorePassword() !=null) {
                        keyStoreOptions.setPassword(new String(options.getTlsKeystorePassword()));
                    }
                    clientOptions.setPfxKeyCertOptions(keyStoreOptions);
                }
            }


            if (options.getTlsTruststorePath() != null) {
                final String path = options.getTlsTruststorePath();
                if (path.endsWith("jks")) {
                    final JksOptions jksOptions = new JksOptions().setPath(path);
                    if (options.getTlsKeystorePassword() !=null) {
                        jksOptions.setPassword(new String(options.getTlsTruststorePassword()));
                    }
                    clientOptions.setTrustStoreOptions(jksOptions);
                } else if (path.endsWith("pfx")) {
                    final PfxOptions pfxOptions = new PfxOptions().setPath(path);
                    if (options.getTlsKeystorePassword() !=null) {
                        pfxOptions.setPassword(new String(options.getTlsTruststorePassword()));
                    }
                    clientOptions.setPfxTrustOptions(pfxOptions);
                }
            }

            client = vertx.createNetClient(clientOptions);

            client.connect(port, host, this::connect);
        }


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
        if (ownVertx) {
            vertx.close();
        }
    }

    @Override
    public void close() throws IOException {
        Future<Void> close = this.client.close();
        close.result();
        if (ownVertx) {
            vertx.close();
        }
    }

    @Override
    public void flush() throws IOException {
    }
    @Override
    public boolean supportsPush() {
        return true;
    }

    public void setReader(final NatsConnectionReader reader){
        this.reader = reader;
    }

    @Override
    public void setWriter(NatsConnectionWriter writer){
        this.writer = writer;
    }

    @Override
    public void setNatsConnection(final NatsConnection connection){
        this.connection = connection;
    }
}
