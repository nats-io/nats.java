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
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;

import io.nats.client.Options;

public class SocketChannelDataPort implements DataPort {

    private NatsConnection connection;
    private String host;
    private int port;

    private SocketChannel socketChannel;
    private SSLEngine engine;
    private ByteBuffer readBuffer;
    private ByteBuffer unwrapBuffer;
    private ByteBuffer writeBuffer;

    private ExecutorService executor;

    public SocketChannelDataPort() {
        this(NatsConnection.BUFFER_SIZE);
    }

    public SocketChannelDataPort(int initialBufferSize) {
        readBuffer = ByteBuffer.allocate(initialBufferSize);
        unwrapBuffer = ByteBuffer.allocate(initialBufferSize);
        writeBuffer = ByteBuffer.allocate(initialBufferSize);
        executor = Executors.newSingleThreadExecutor();
    }

    public void connect(String serverURI, NatsConnection conn) throws IOException {
        try {
            this.connection = conn;

            Options options = this.connection.getOptions();
            this.socketChannel = SocketChannel.open();
            this.socketChannel.configureBlocking(false);

            URI uri = new URI(serverURI);
            long timeout = options.getConnectionTimeout().toNanos();

            this.host = uri.getHost();
            this.port = uri.getPort();
            this.socketChannel.connect(new InetSocketAddress(this.host, this.port));

            long start = System.nanoTime();
            while (!this.socketChannel.finishConnect()) {
                long now = System.nanoTime();
                if (now - start >= timeout) {
                    throw new IOException("Connection timed out");
                }

                try {
                    Thread.sleep(0, 50); // Sleep a very short time
                } catch (Exception ex) {
                    // Ignore sleep exceptions
                }
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    /**
     * Upgrade the port to SSL. If it is already secured, this is a no-op. If the
     * data port type doesn't support SSL it should throw an exception.
     */
    public void upgradeToSecure() throws IOException {
        Options options = this.connection.getOptions();
        SSLContext context = options.getSslContext();

        engine = context.createSSLEngine(this.host, this.port);
        engine.setUseClientMode(true);

        engine.beginHandshake();

        this.readBuffer.clear();
        this.writeBuffer.clear();

        tryToHandshake();

        this.readBuffer.clear().flip();
        this.writeBuffer.clear();
        this.unwrapBuffer.clear().flip();
    }

    void tryToHandshake() throws IOException {

        // Use the read buffer for the "network" version
        // Use the write buffer for the internal version
        readBuffer.clear();
        writeBuffer.clear();

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
            case FINISHED:
                break;
            case NOT_HANDSHAKING:
                throw new IllegalStateException("Not handshaking status while handshaking.");
            default:
                throw new IllegalStateException("Invalid SSL handshake status: " + handshake);
            }
        }
    }

    HandshakeStatus handleHandshakeTask() throws IOException {
        Runnable task;
        while ((task = engine.getDelegatedTask()) != null) {
            executor.execute(task);
        }
        return engine.getHandshakeStatus();
    }

    HandshakeStatus handleHandshakeUnwrap() throws IOException {
        SSLEngineResult result;
        HandshakeStatus status = HandshakeStatus.NEED_UNWRAP;

        if (readBuffer.position() == readBuffer.limit()) {
            readBuffer.clear();
        }

        if (this.socketChannel.read(readBuffer) < 0) {
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

        try {
            this.readBuffer.flip();
            this.writeBuffer.clear();
            result = engine.unwrap(this.readBuffer, this.writeBuffer);
            readBuffer.compact();

            status = result.getHandshakeStatus();
        } catch (SSLException e) {
            engine.closeOutbound();
            return engine.getHandshakeStatus();
        }

        switch (result.getStatus()) {
        case OK:
            break;
        case BUFFER_OVERFLOW:
            this.writeBuffer = ByteBuffer.allocate(this.writeBuffer.capacity() * 2);
            break;
        case BUFFER_UNDERFLOW:
            if (this.readBuffer.limit() < engine.getSession().getPacketBufferSize()) {
                ByteBuffer newReadBuffer = ByteBuffer.allocate(this.readBuffer.capacity() * 2);
                this.readBuffer.flip();
                newReadBuffer.put(this.readBuffer);
                this.readBuffer = newReadBuffer;
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

    HandshakeStatus handleHandshakeWrap() throws IOException {
        SSLEngineResult result;
        HandshakeStatus status = HandshakeStatus.NEED_WRAP;

        try {
            this.writeBuffer.flip();
            this.readBuffer.clear();
            result = engine.wrap(this.writeBuffer, this.readBuffer);
            this.readBuffer.flip();

            status = engine.getHandshakeStatus();
        } catch (SSLException e) {
            engine.closeOutbound();
            return engine.getHandshakeStatus();
        }

        switch (result.getStatus()) {
        case OK:
            while (this.readBuffer.hasRemaining()) {
                this.socketChannel.write(this.readBuffer);
            }
            break;
        case BUFFER_OVERFLOW:
            readBuffer = ByteBuffer.allocate(this.readBuffer.capacity() * 2);
            break;
        case BUFFER_UNDERFLOW:
            throw new IOException("Underflow during handshake wrap.");
        case CLOSED:
            try {
                while (readBuffer.hasRemaining()) {
                    this.socketChannel.write(readBuffer);
                }
                writeBuffer.clear();
            } catch (Exception ex) {
                status = engine.getHandshakeStatus();
            }
            break;
        default:
            throw new IllegalStateException("Unknown SSL Status: " + result.getStatus());
        }

        return status;
    }

    int copyFromUnwrapped(ByteBuffer dst) throws IOException {
        int bytesCopied = 0;
        long limit = dst.limit();

        while (this.unwrapBuffer.hasRemaining() && bytesCopied < limit) {
            dst.put(this.unwrapBuffer.get());
            bytesCopied++;
        }

        return bytesCopied;
    }

    int processReadBuffer(ByteBuffer dst) throws IOException {
        while (this.readBuffer.hasRemaining()) {
            this.unwrapBuffer.clear();
            SSLEngineResult result = engine.unwrap(this.readBuffer, this.unwrapBuffer);
            this.unwrapBuffer.flip();

            switch (result.getStatus()) {
            case OK:
                return copyFromUnwrapped(dst);
            case BUFFER_OVERFLOW:
                this.unwrapBuffer = ByteBuffer.allocate(this.unwrapBuffer.capacity() * 2);
                this.unwrapBuffer.flip(); // so it is empty
                break;
            case BUFFER_UNDERFLOW:
                if (this.readBuffer.limit() < engine.getSession().getPacketBufferSize()) {
                    ByteBuffer newReadBuffer = ByteBuffer.allocate(this.readBuffer.capacity() * 2);
                    this.readBuffer.flip();
                    newReadBuffer.put(this.readBuffer);
                    this.readBuffer = newReadBuffer;
                }

                int tmpRead = this.socketChannel.read(this.readBuffer); // will append to what we have
                this.readBuffer.flip();

                if (tmpRead < 0) {
                    throw new IOException("Underlying socket connection is closed.");
                }

                break; // Continue the loop
            case CLOSED:
                throw new IOException("Underlying SSL connection is closed.");
            default:
                throw new IllegalStateException("Invalid SSL Status: " + result.getStatus());
            }
        }

        return 0;
    }

    // In secure mode, the data port proxies the socket's buffer to insure that
    // we can unwrap ssl data properly. The DataPort will return values <= 0 when complete
    // like the underlying socket channel, but may return a value less than the
    // actual amount read from the socket channel if the dst buffer is too small.
    public int read(ByteBuffer dst) throws IOException {
        if (this.engine == null) {
            return this.socketChannel.read(dst);
        }

        int bytesRead = 0;

        if (this.unwrapBuffer.hasRemaining()) {
            bytesRead = copyFromUnwrapped(dst);
        } else if (this.readBuffer.hasRemaining()) {
            bytesRead = processReadBuffer(dst);
        } 
        
        if (bytesRead == 0) {
            this.readBuffer.clear();
            int tmpRead = this.socketChannel.read(this.readBuffer);
            this.readBuffer.flip();

            // If we got data, reset the bytes read to reflect what we copied.
            if (tmpRead > 0) {
                bytesRead = processReadBuffer(dst);
            }
        }

        return bytesRead;
    }

    public void write(ByteBuffer src) throws IOException {
        if (this.engine == null) {
            this.socketChannel.write(src);
            return;
        }

        while (src.hasRemaining()) {
            this.writeBuffer.clear();
            SSLEngineResult result = this.engine.wrap(src, this.writeBuffer);
            switch (result.getStatus()) {
            case OK:
                writeBuffer.flip();
                while (writeBuffer.hasRemaining()) {
                    this.socketChannel.write(writeBuffer);
                }
                break;
            case BUFFER_OVERFLOW:
                this.writeBuffer = ByteBuffer.allocate(this.writeBuffer.capacity() * 2);
                break;
            case BUFFER_UNDERFLOW:
                throw new IOException("Buffer underflow from wrap.");
            case CLOSED:
                throw new IOException("Underlying SSL connection is closed.");
            default:
                throw new IllegalStateException("Invalid SSL Status: " + result.getStatus());
            }
        }
    }

    public void close() throws IOException {
        executor.shutdown();
        if (engine != null) {
            engine.closeOutbound();
        }
        this.socketChannel.close();
    }
}