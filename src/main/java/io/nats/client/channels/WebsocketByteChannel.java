// Copyright 2021 The NATS Authors
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

package io.nats.client.channels;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.nio.channels.ByteChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.nats.client.http.HttpInterceptor;
import io.nats.client.http.HttpRequest;
import io.nats.client.http.HttpResponse;
import io.nats.client.impl.Headers;
import io.nats.client.support.WebsocketFrameHeader.OpCode;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Implements the websocket protocol. Thread safe.
 */
public class WebsocketByteChannel implements ByteChannel, GatheringByteChannel {
    private static final ByteBuffer[] EMPTY = new ByteBuffer[0];

    public enum Role {
        CLIENT,
        SERVER;
    }

    private enum State {
        HANDSHAKE,
        OPEN,
        SEND_CLOSING,
        WAITING_FOR_CLOSE,
        CLOSED;
    }

    @FunctionalInterface
    private interface Handshake {
        void handshake() throws IOException;
    }

    private final WebsocketReadableByteChannel reader;
    private final WebsocketWritableByteChannel writer;
    private final OpCode defaultOpCode;

    // Acquire always in this order: readerLock > writerLock > stateLock
    private Lock readerLock = new ReentrantLock();
    private Lock writerLock = new ReentrantLock();
    private Object stateLock = new Object();

    // Protected by stateLock:
    private State state = State.HANDSHAKE;
    private Thread readThread = null;
    private Thread writeThread = null;

    // Fields for handshake, protected via readerLock + writerLock:
    private String acceptKey;
    private HttpRequest httpRequest;
    private HttpInterceptor.Chain httpInterceptorChain;

    /**
     * @param writer is the channel to use for writing.
     * @param reader is the channel to use for reading.
     * @param defaultOpCode is the default weboscket OpCode to use, this may only be
     *     OpCode.TEXT or OpCode.BINARY.
     * @param rand is the Random number generator to use for various operations.
     * @param httpInterceptors is a list of interceptors to use when performing the
     *    initial handshake. Useful if writing an auth or proxy mechanism.
     * @param serverURI is the URI of the server to connect to.
     * @param role is weather this byte channel is operating as a client or a server.
     *    Specifically, when operating as a client, all outbound information is masked.
     */
    public WebsocketByteChannel(
        GatheringByteChannel writer,
        ReadableByteChannel reader,
        OpCode defaultOpCode,
        Random rand,
        List<HttpInterceptor> httpInterceptors,
        URI serverURI,
        Role role)
    {
        assert null != role;

        ByteBuffer readBuffer = ByteBuffer.allocate(10 * 1024);

        this.defaultOpCode = defaultOpCode;
        this.writer = new WebsocketWritableByteChannel(writer, role == Role.CLIENT, defaultOpCode, rand);
        this.reader = new WebsocketReadableByteChannel(reader, readBuffer);
        this.httpInterceptorChain = HttpInterceptor.buildChain(httpInterceptors, writer, reader, readBuffer);

        String nonce = createNonce(rand);
        String uri = (null == serverURI.getRawQuery())
            ? serverURI.getRawPath()
            : serverURI.getRawPath() + "?" + serverURI.getRawQuery();
        if ("".equals(uri)) {
            uri = "/";
        }
        this.acceptKey = createAcceptKeyForNonce(nonce);
        this.httpRequest = HttpRequest.builder()
            .uri(uri)
            .headers(new Headers()
                .add("Host", serverURI.getHost())
                .add("Upgrade", "websocket")
                .add("Connection", "Upgrade")
                .add("Sec-WebSocket-Key", nonce)
                .add("Sec-WebSocket-Protocol", "nats")
                .add("Sec-WebSocket-Version", "13"))
            .build();

    }

    /**
     * Force a handshake to take place immediately rather than waiting
     * for the first read or write call.
     *
     * @throws IOException if there was an I/O exception during read/writes
     *    necessary for handshaking.
     */
    public void handshake() throws IOException {
        synchronized (stateLock) {
            if (state != State.HANDSHAKE) {
                return;
            }
        }
        readerLock.lock();
        try {
            synchronized (stateLock) {
                readThread = Thread.currentThread();
            }
            writerLock.lock();
            try {
                synchronized (stateLock) {
                    writeThread = Thread.currentThread();
                }
                handshakeImpl();
            } finally {
                synchronized (stateLock) {
                    writeThread = null;
                }
                writerLock.unlock();
            }
        } finally {
            synchronized (stateLock) {
                readThread = null;
            }
            readerLock.unlock();
        }
    }

    /**
     * Reads at most one websocket frame. Note that the frame may be empty in
     * which case the read will return 0.
     * 
     * Note that the first read after the header may be a "short" read. This
     * is necessary to avoid an unintentional blocking read.
     * 
     * @param dst is the buffer populate with the websocket frame body.
     * @return the number of bytes populated or -1 if the wrapped channel got closed.
     * @throws IOException if there is an input/output error.
     */
    @Override
    public int read(ByteBuffer dst) throws IOException {
        return read(dst, null);
    }

    /**
     * Performs a read followed by a call to the specified op code consumer
     * in the event that a new websocket header was read.
     * 
     * @param dst is the buffer populate with the websocket frame body.
     * @param opCodeConsumer is the function to call IF there is a new websocket frame.
     * @return the number of bytes populated or -1 if the wrapped channel got closed.
     * @throws IOException if there is an input/output error.
     */
    public int read(ByteBuffer dst, Consumer<OpCode> opCodeConsumer) throws IOException {
        handshake();
        OpCode[] opCode = new OpCode[1];
        int result;
        readerLock.lock();
        try {
            synchronized (stateLock) {
                readThread = Thread.currentThread();
                switch (state) {
                case WAITING_FOR_CLOSE:
                case OPEN:
                    break;
                default:
                    return -1;
                }
            }
            result = reader.read(dst, code -> opCode[0] = code);
            if (OpCode.CLOSE == opCode[0] || result < 0) {
                boolean transitionedToClose = false;
                synchronized (stateLock) {
                    if (State.WAITING_FOR_CLOSE == state) {
                        state = State.CLOSED;
                        transitionedToClose = true;
                    } else {
                        state = State.SEND_CLOSING;
                    }    
                }
                if (transitionedToClose) {
                    try {
                        reader.close();
                    } finally {
                        writer.close();
                    }
                }
            }
        } finally {
            synchronized (stateLock) {
                readThread = null;
            }
            readerLock.unlock();
        }
        if (null != opCodeConsumer && null != opCode[0]) {
            // Ensure not to make foreign call with locks held:
            opCodeConsumer.accept(opCode[0]);
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        return write(srcs, offset, length, defaultOpCode, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int write(ByteBuffer src) throws IOException {
        return Math.toIntExact(write(new ByteBuffer[]{src}, 0, 1));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return write(srcs, 0, 1);
    }

    /**
     * Peforms a write of a websocket frame with the specified op code and final fields.
     * 
     * @param srcs a list of buffers in "get" mode that need to be written. Note
     *     that these buffers may be written to, so you should copy any data out
     *     of the buffer if it needs to be preserved across invocations.
     * @param offset is the start offset within the list of srcs to write.
     * @param length is the total number of buffers within the list to process.
     * @param code is the OpCode to use for the header.
     * @param isFinalFragment should only be false if fragmenting a message into
     *     multiple frames and this is NOT the final fragment. Use in combination
     *     with OpCode.CONTINUATION to split up a single logical payload into
     *     smaller chunks.
     * @return the number of bytes written from src, which will always be the
     *    remaining size of the source unless the end of stream was observed.
     * @throws ReadOnlyBufferException if stream is in Role.CLIENT and a readonly
     *    ByteBuffer is specified in srcs.
     * @throws IOException if there is an input/output error.
     */
    public int write(ByteBuffer[] srcs, int offset, int length, OpCode code, boolean isFinalFragment) throws IOException {
        handshake();
        writerLock.lock();
        try {
            synchronized (stateLock) {
                writeThread = Thread.currentThread();
            }
            if (State.SEND_CLOSING == state) {
                writer.write(EMPTY, 0, 0, OpCode.CLOSE, true);
                return -1;
            } else if (State.OPEN == state) {
                return (int)writer.write(srcs, 0, 1, code, isFinalFragment);
            } else {
                return -1;
            }
        } finally {
            synchronized (stateLock) {
                writeThread = null;
            }
            writerLock.unlock();
        }
    }

    /**
     * @return true if either ther reader or writer channels is still open.
     */
    @Override
    public boolean isOpen() {
        return reader.isOpen() || writer.isOpen();
    }

    /**
     * If open, sends a websocket close message with an empty body and waits for a
     * close frame response.
     * @throws IOException if there is an input/output error.
     */
    @Override
    public void close() throws IOException {
        synchronized (stateLock) {
            if (null != readThread) {
                readThread.interrupt();
            }
            if (null != writeThread) {
                writeThread.interrupt();
            }
        }
        readerLock.lock();
        try {
            // Do NOT set readThread/writeThread, since it would be better
            // to have the complete close be handled by a single
            // thread.
            writerLock.lock();
            try {
                closeImpl();
            } finally {
                writerLock.unlock();
            }
        } finally {
            readerLock.unlock();
        }
    }

    private void handshakeImpl() throws IOException {
        synchronized (stateLock) {
            if (state != State.HANDSHAKE) {
                return;
            }
        }
        HttpResponse response;
        try {
            response = httpInterceptorChain.proceed(httpRequest);
        } catch (Exception ex) {
            throw new InvalidWebsocketProtocolException("Issue writing HTTP request and/or reading the response", ex);
        }
        Headers headers = response.getHeaders();

        // rfc6455 4.1 "The client MUST validate the server's response as follows:"
        // 1. status 101
        if (response.getStatusCode() != 101) {
            throw new InvalidWebsocketProtocolException("Expected status code 101, but got " + response.getStatusCode());
        }

        // 2. Expect `Upgrade: websocket`
        if (!headers.getIgnoreCase("upgrade").stream().anyMatch("websocket"::equalsIgnoreCase)) {
            throw new InvalidWebsocketProtocolException(
                "Expected HTTP `Upgrade: websocket` header");
        }

        // 3. Expect `Connection: Upgrade`
        if (!headers.getIgnoreCase("connection").stream().anyMatch("upgrade"::equalsIgnoreCase)) {
            throw new InvalidWebsocketProtocolException(
                "Expected HTTP `Connection: Upgrade` header");
        }

        // 4. Sec-WebSocket-Accept: base64(sha1(key + "258EAF..."))
        if (!headers.getIgnoreCase("sec-websocket-accept").stream().anyMatch(acceptKey::equals)) {
            throw new InvalidWebsocketProtocolException(
                "Expected HTTP `Sec-WebSocket-Accept: " + acceptKey + ", but got [" +
                headers.getIgnoreCase("sec-websocket-accept").stream().collect(Collectors.joining("; ")) +
                "]");
        }
        // 5 & 6 are not valid, since nats-server doesn't
        // implement extensions or protocols.

        // Release memory that is no longer required and transition to OPEN state:
        synchronized (stateLock) {
            acceptKey = null;
            httpRequest = null;
            httpInterceptorChain = null;
            state = State.OPEN;
        }
    }

    private void closeImpl() throws IOException {
        while (true) {
            boolean needsRead = false;
            boolean needsWrite = false;
            synchronized (stateLock) {
                switch (state) {
                case OPEN:
                case SEND_CLOSING:
                    state = State.SEND_CLOSING;
                    needsWrite = true;
                    break;
                case WAITING_FOR_CLOSE:
                    state = State.WAITING_FOR_CLOSE;
                    needsRead = true;
                    break;
                case HANDSHAKE:
                case CLOSED:
                    return;
                }
            }
            if (needsRead) {
                ByteBuffer discard = ByteBuffer.allocate(1024);
                while (read(discard) > 0) {
                    synchronized (stateLock) {
                        if (State.CLOSED == state) {
                            reader.close();
                            writer.close();
                            return;
                        }
                    }
                }
            } else if (needsWrite) {
                writer.write(EMPTY, 0, 0, OpCode.CLOSE, true);
                synchronized (stateLock) {
                    state = State.WAITING_FOR_CLOSE;
                }
            }
        }
    }

    private static String createNonce(Random rand) {
        byte[] keyBytes = new byte[16];
        rand.nextBytes(keyBytes);
        return Base64.getEncoder().encodeToString(keyBytes);
    }

    private static String createAcceptKeyForNonce(String nonce) {
        MessageDigest sha1;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException(ex); // JDK should always provide this algorithm
        }
        sha1.update(nonce.getBytes(UTF_8));
        // This value is in the RFC for websockets:
        sha1.update("258EAFA5-E914-47DA-95CA-C5AB0DC85B11".getBytes(UTF_8));
        return Base64.getEncoder().encodeToString(sha1.digest());
    }

}
