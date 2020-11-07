// Copyright 2015-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.client.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.concurrent.*;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;

import io.nats.client.Options;

import static java.net.StandardSocketOptions.*;

public class SocketDataPort implements DataPort {

    private final ByteBuffer EMPTY = ByteBuffer.allocate(0);

    private NatsConnection connection;
    private String host;
    private int port;
    private AsynchronousSocketChannel socket;
    private SSLEngine engine;
    private ByteBuffer readBuffer;
    private ByteBuffer unwrapBuffer;
    private ByteBuffer writeBuffer;

    private ExecutorService executor;

    public CompletableFuture<DataPort> connect(String serverURI, NatsConnection conn, long timeoutNanos) throws IOException {

        try {
            this.connection = conn;

            Options options = this.connection.getOptions();

            URI uri = options.createURIForServer(serverURI);
            this.host = uri.getHost();
            this.port = uri.getPort();

            // Create a new future for the dataport, the reader/writer will use this
            // to wait for the connect/failure.
            CompletableFuture<DataPort> dataPortFuture = new CompletableFuture<>();

            this.socket = AsynchronousSocketChannel.open();
            socket.setOption(TCP_NODELAY, true);
            socket.setOption(SO_RCVBUF, 2 * 1024 * 1024);
            socket.setOption(SO_SNDBUF, 2 * 1024 * 1024);
            socket.connect(new InetSocketAddress(host, port), dataPortFuture,
                    new CompletionHandler<Void, CompletableFuture<DataPort>>() {
                        public void completed(Void v1, CompletableFuture<DataPort> dataPortFuture) {
                            // Notify the any threads waiting on the sockets
                            dataPortFuture.complete(conn.getDataPort());
                        }
                        public void failed(Throwable th, CompletableFuture<DataPort> dataPortFuture) {
                            dataPortFuture.completeExceptionally(th);
                        }
                    });
            return dataPortFuture;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    /**
     * Upgrade the port to SSL. If it is already secured, this is a no-op. If the
     * data port type doesn't support SSL it should throw an exception.
     */
    public void upgradeToSecure() throws IOException, ExecutionException, InterruptedException {
        Options options = this.connection.getOptions();
        SSLContext context = options.getSslContext();

        engine = context.createSSLEngine(this.host, this.port);
        engine.setUseClientMode(true);
        engine.beginHandshake();

        if (this.executor == null)
            this.executor = Executors.newSingleThreadExecutor();

        if (this.readBuffer == null)
            this.readBuffer = ByteBuffer.allocate(options.getBufferSize());

        if (this.writeBuffer == null)
            this.writeBuffer = ByteBuffer.allocate(options.getBufferSize());

        if (this.unwrapBuffer == null)
            this.unwrapBuffer = ByteBuffer.allocate(options.getBufferSize());

        tryToHandshake();

        this.readBuffer.clear().flip();
        this.writeBuffer.clear();
        this.unwrapBuffer.clear().flip();
    }

    void tryToHandshake() throws IOException, ExecutionException, InterruptedException {

        // Use the read buffer for the "network" version
        // Use the write buffer for the internal version
        this.readBuffer.clear();
        //this.readBuffer.position(this.readBuffer.capacity());
        this.writeBuffer.clear();

        HandshakeStatus handshake = engine.getHandshakeStatus();

        while (handshake != HandshakeStatus.FINISHED) {
            switch (handshake) {
                case NEED_UNWRAP:
                    handshake = handleHandshakeUnwrap();
                    break;
                case NEED_WRAP:
                    handshake = handleHandshakeWrap();
                    break;
                case NEED_TASK:
                    handshake = handleHandshakeTask();
                    break;
                case NOT_HANDSHAKING:
                    throw new IllegalStateException("Not handshaking status while handshaking.");
                default:
                    throw new IllegalStateException("Invalid SSL handshake status: " + handshake);
            }
        }
    }

    HandshakeStatus handleHandshakeTask() {
        Runnable task;
        while ((task = engine.getDelegatedTask()) != null) {
            executor.execute(task);
        }
        return engine.getHandshakeStatus();
    }
    HandshakeStatus handleHandshakeUnwrap() throws IOException, ExecutionException, InterruptedException {
        SSLEngineResult result = null;
        HandshakeStatus status = HandshakeStatus.NEED_UNWRAP;

        writeBuffer.clear();

        while (status == HandshakeStatus.NEED_UNWRAP) {
            try {
                this.readBuffer.flip();
                if (readBuffer.position() != readBuffer.limit()) {
                    result = engine.unwrap(this.readBuffer, this.writeBuffer);
                    status = result.getHandshakeStatus();
                }
                this.readBuffer.compact();
            } catch (SSLException e) {
                engine.closeOutbound();
                return engine.getHandshakeStatus();
            }
            if (status != HandshakeStatus.NEED_UNWRAP || (result != null && result.getStatus() == SSLEngineResult.Status.OK))
                break;
            Future<Integer> readFut = this.socket.read(readBuffer);
            int read = readFut.get();
            if (read < 0) {
                if (engine.isInboundDone() && engine.isOutboundDone()) {
                    throw new IOException("Connection is shutdown");
                }

                try {
                    engine.closeInbound();
                } catch (SSLException e) {
                    // ignore
                }

                engine.closeOutbound();
                return engine.getHandshakeStatus();
            }
        }

        switch (result.getStatus()) {
            case OK:
                break;
            case BUFFER_OVERFLOW:
                this.writeBuffer = ByteBuffer.allocate(this.writeBuffer.capacity() * 2);
                break;
            case BUFFER_UNDERFLOW:
                if (this.readBuffer.limit() < engine.getSession().getPacketBufferSize()) {
                    this.readBuffer = this.connection.enlargeBuffer(this.readBuffer, 0);
                }
                break;
            case CLOSED:
                if (this.engine.isOutboundDone()) {
                    throw new IOException("Outbound side of SSL connection is already closed");
                } else {
                    engine.closeOutbound();
                    status = engine.getHandshakeStatus();
                    break;
                }
            default:
                throw new IllegalStateException("Unknown SSL Status: " + result.getStatus());
        }

        return status;
    }

    HandshakeStatus handleHandshakeWrap() throws IOException, ExecutionException, InterruptedException {
        SSLEngineResult result;
        HandshakeStatus status;

        this.writeBuffer.clear();

        try {
            result = engine.wrap(EMPTY, this.writeBuffer);

            status = result.getHandshakeStatus();
        } catch (SSLException e) {
            e.printStackTrace();
            engine.closeOutbound();
            return engine.getHandshakeStatus();
        }

        switch (result.getStatus()) {
            case OK:
                this.writeBuffer.flip();
                while (this.writeBuffer.hasRemaining()) {
                    int write = this.socket.write(this.writeBuffer).get();
                    if (write < 0)
                        throw new IOException("Underlying SSL connection is closed.");
                }
                break;
            case BUFFER_OVERFLOW:
                writeBuffer = ByteBuffer.allocate(this.writeBuffer.capacity() * 2);
                break;
            case BUFFER_UNDERFLOW:
                throw new IOException("Underflow during handshake wrap.");
            case CLOSED:
                this.writeBuffer.flip();
                try {
                    while (this.writeBuffer.hasRemaining()) {
                        int write = this.socket.write(this.writeBuffer).get();
                        if (write < 0)
                            throw new IOException("Underlying SSL connection is closed.");
                    }
                } catch (Exception ex) {
                    status = engine.getHandshakeStatus();
                } finally {
                    this.writeBuffer.clear();
                }
                break;
            default:
                throw new IllegalStateException("Unknown SSL Status: " + result.getStatus());
        }

        return status;
    }

    CompletableFuture<Integer> copyFromUnwrapped(ByteBuffer dst) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        int bytesCopied;

        if (this.unwrapBuffer.remaining() < dst.remaining()) {
            bytesCopied = this.unwrapBuffer.remaining();
            dst.put(this.unwrapBuffer);
        } else {
            bytesCopied = dst.remaining();
            int limit = this.unwrapBuffer.limit();
            this.unwrapBuffer.limit(this.unwrapBuffer.position() + dst.remaining());
            dst.put(this.unwrapBuffer);
            this.unwrapBuffer.limit(limit);
        }

        future.complete(bytesCopied);
        return future;
    }

    CompletableFuture<Integer> processReadBuffer(ByteBuffer dst) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        while (dst.hasRemaining() && this.readBuffer.hasRemaining()) {
            SSLEngineResult result = null;
            try {
                result = engine.unwrap(this.readBuffer, this.unwrapBuffer);
            } catch (SSLException e) {
                e.printStackTrace();
                future.completeExceptionally(e);
                return future;
            }

            switch (result.getStatus()) {
                case OK:
                    this.unwrapBuffer.flip();
                    return copyFromUnwrapped(dst);
                case BUFFER_OVERFLOW:
                    this.unwrapBuffer = ByteBuffer.allocate(this.unwrapBuffer.capacity() * 2);
                    this.unwrapBuffer.flip(); // so it is empty
                    break;
                case BUFFER_UNDERFLOW:
                    this.readBuffer.compact();
                    if (this.readBuffer.limit() < engine.getSession().getPacketBufferSize()) {
                        this.readBuffer = this.connection.enlargeBuffer(this.readBuffer, 0);
                    }

                    Future<Integer> readFut = this.socket.read(this.readBuffer);
                    int tmpRead = 0; // will append to what we have
                    try {
                        tmpRead = readFut.get();
                    } catch (InterruptedException | ExecutionException e) {
                        future.completeExceptionally(e);
                        return future;
                    }

                    if (tmpRead < 0) {
                        future.complete(tmpRead);
                        return future;
                    }
                    this.readBuffer.flip();
                    break; // Continue the loop
                case CLOSED:
                    future.completeExceptionally(new IOException("Underlying SSL connection is closed."));
                    return future;
                default:
                    throw new IllegalStateException("Invalid SSL Status: " + result.getStatus());
            }
        }

        return future;
    }

    public CompletableFuture<Integer> read(ByteBuffer dst) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        if (this.engine == null) {
            socket.read(dst, future,
                    new CompletionHandler<Integer, CompletableFuture<Integer>>() {
                        public void completed(Integer read, CompletableFuture<Integer> future) {
                            future.complete(read);
                        }

                        public void failed(Throwable th, CompletableFuture<Integer> future) {
                            future.completeExceptionally(th);
                        }
                    }
            );
            return future;
        }
        int bytesRead = 0;

        if (this.unwrapBuffer.hasRemaining()) {
            CompletableFuture<Integer> copyFut = copyFromUnwrapped(dst);
            try {
                bytesRead = copyFut.get();
            } catch (InterruptedException | ExecutionException e) {
                return copyFut;
            }
        } else if (this.readBuffer.hasRemaining()) {
            this.unwrapBuffer.compact();
            CompletableFuture<Integer> processFut = processReadBuffer(dst);
            try {
                bytesRead = processFut.get();
            } catch (InterruptedException | ExecutionException e) {
                return processFut;
            }
        }

        if (bytesRead == 0) {
            this.readBuffer.compact();
            Future<Integer> readFut = this.socket.read(this.readBuffer);
            int tmpRead = 0;
            try {
                tmpRead = readFut.get();
            } catch (InterruptedException | ExecutionException e) {
                future.completeExceptionally(e);
                return future;
            }
            this.readBuffer.flip();

            if (tmpRead < 0) {
                future.complete(tmpRead);
                return future;
            }

            // If we got data, reset the bytes read to reflect what we copied.
            if (tmpRead > 0) {
                this.unwrapBuffer.compact();
                CompletableFuture<Integer> processFut = processReadBuffer(dst);
                try {
                    bytesRead = processFut.get();
                } catch (InterruptedException | ExecutionException e) {
                    return processFut;
                }
            }
        }
        future.complete(bytesRead);
        return future;
    }

    public CompletableFuture<Integer> write(ByteBuffer src) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        if (this.engine == null) {
            socket.write(src, future,
                    new CompletionHandler<Integer, CompletableFuture<Integer>>() {
                        public void completed(Integer write, CompletableFuture<Integer> future) {
                            future.complete(write);
                        }
                        public void failed(Throwable th, CompletableFuture<Integer> future) {
                            future.completeExceptionally(th);
                        }
                    }
            );
            return future;
        }
        int consumed = 0;
        while (src.hasRemaining()) {
            this.writeBuffer.clear();
            SSLEngineResult result = null;
            try {
                result = this.engine.wrap(src, this.writeBuffer);
                consumed += result.bytesConsumed();
            } catch (SSLException e) {
                e.printStackTrace();
                future.completeExceptionally(e);
                return future;
            }
            switch (result.getStatus()) {
                case OK:
                    writeBuffer.flip();
                    while (writeBuffer.hasRemaining()) {
                        Future<Integer> writeFut = this.socket.write(writeBuffer);
                        try {
                            int write = writeFut.get();
                            if (write < 0) {
                                future.complete(write);
                                return future;
                            }
                        } catch (InterruptedException | ExecutionException e) {
                            future.completeExceptionally(e);
                            return future;
                        }
                    }
                    break;
                case BUFFER_OVERFLOW:
                    this.writeBuffer = ByteBuffer.allocate(this.writeBuffer.capacity() * 2);
                    break;
                case BUFFER_UNDERFLOW:
                    future.completeExceptionally(new IOException("Buffer underflow from wrap."));
                    return future;
                case CLOSED:
                    future.completeExceptionally(new IOException("Underlying SSL connection is closed."));
                    return future;
                default:
                    future.completeExceptionally(new IllegalStateException("Invalid SSL Status: " + result.getStatus()));
                    return future;
            }
        }
        future.complete(consumed);
        return future;
    }

    public void close() throws IOException {
        if (executor != null) {
            executor.shutdown();
        }
        if (engine != null) {
            if (engine.isOutboundDone())
                engine.closeOutbound();
            if (engine.isInboundDone())
                engine.closeInbound();
            engine = null;
        }
        if (socket.isOpen()) {
            socket.close();
        }
        this.readBuffer = null;
        this.writeBuffer = null;
        this.unwrapBuffer = null;
    }
}
