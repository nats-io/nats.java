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
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.GatheringByteChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;

import static  io.nats.client.support.BufferUtils.append;
import static  io.nats.client.support.BufferUtils.remaining;

/**
 * It blows my mind that JDK doesn't provide this functionality by default.
 * 
 * This is an implementation of ByteChannel which uses an SSLEngine to encrypt data sent
 * to a ByteChannel that is being wrapped, and then decrypts received data.
 */
public class TLSByteChannel implements ByteChannel, GatheringByteChannel {
    private static final ByteBuffer[] EMPTY = new ByteBuffer[]{ByteBuffer.allocate(0)};

    private final ByteChannel wrap;
    private final SSLEngine engine;

    // NOTE: Locks should always be acquired in this order:
    // readLock > writeLock > stateLock
    private final Lock readLock = new ReentrantLock();
    private final Lock writeLock = new ReentrantLock();

    // All of the below state is controlled with this lock:
    private final Object stateLock = new Object();
    private Thread readThread = null;
    private Thread writeThread = null;
    private State state;
    // end state protected by the stateLock

    private final ByteBuffer outNetBuffer; // in "get" mode, protected by writeLock

    private final ByteBuffer inNetBuffer; // in "put" mode, protected by readLock
    private final ByteBuffer inAppBuffer; // in "put" mode, protected by readLock


    private enum State {
        HANDSHAKING_READ,
        HANDSHAKING_WRITE,
        HANDSHAKING_TASK,
        OPEN,
        CLOSING,
        CLOSED;
    }

    public TLSByteChannel(ByteChannel wrap, SSLEngine engine) throws IOException {
        this.wrap = wrap;
        this.engine = engine;

        int netBufferSize = engine.getSession().getPacketBufferSize();
        int appBufferSize = engine.getSession().getApplicationBufferSize();

        outNetBuffer = ByteBuffer.allocate(netBufferSize);
        outNetBuffer.flip();

        inNetBuffer = ByteBuffer.allocate(netBufferSize);
        inAppBuffer = ByteBuffer.allocate(appBufferSize);

        engine.beginHandshake();
        state = toState(engine.getHandshakeStatus());
    }

    /**
     * Translate an SSLEngine.HandshakeStatus into internal state.
     */
    private static State toState(HandshakeStatus status) {
        switch (status) {
        case NEED_TASK:
            return State.HANDSHAKING_TASK;
        case NEED_UNWRAP:
            return State.HANDSHAKING_READ;
        case NEED_WRAP:
            return State.HANDSHAKING_WRITE;
        case FINISHED:
        case NOT_HANDSHAKING:
            return State.OPEN;
        default:
            throw new IllegalStateException("Unexpected SSLEngine.HandshakeStatus=" + status);
        }
    }

    /**
     * Force a TLS handshake to take place if it has not already happened.
     * @return false if end of file is observed.
     * @throws IOException if any underlying read or write call throws.
     */
    public boolean handshake() throws IOException {
        while (true) {
            boolean needsRead = false;
            boolean needsWrite = false;

            synchronized (stateLock) {
                switch (state) {
                case HANDSHAKING_TASK:
                    executeTasks();
                    state = toState(engine.getHandshakeStatus());
                    break;

                case HANDSHAKING_READ:
                    needsRead = true;
                    break;

                case HANDSHAKING_WRITE:
                    needsWrite = true;
                    break;
                default:
                    return true;
                }
            }

            if (needsRead) {
                if (readImpl(EMPTY[0]) < 0) {
                    return false;
                }
            } else if (needsWrite) {
                if (writeImpl(EMPTY, 0, 1) < 0) {
                    return false;
                }
            }
        }
    }

    /**
     * Gracefully close the TLS session and the underlying wrap'ed socket.
     */
    @Override
    public void close() throws IOException {
        if (!wrap.isOpen()) {
            return;
        }

        // [1] Make sure any handshake has happened:
        handshake();

        // [2] Set state to closing, or return if another thread
        // is already closing.
        synchronized (stateLock) {
            if (State.CLOSED == state || State.CLOSING == state) {
                return;
            } else {
                state = State.CLOSING;
            }

            // [3] Interrupt any reading/writing threads:
            if (null != readThread) {
                readThread.interrupt();
            }
            if (null != writeThread) {
                writeThread.interrupt();
            }
        }

        // [4] Try to acquire readLock:
        try {
            if (!readLock.tryLock(100, TimeUnit.MICROSECONDS)) {
                wrap.close();
                return;
            }
            try {

                // [5] Try to acquire writeLock:
                if (!writeLock.tryLock(100, TimeUnit.MICROSECONDS)) {
                    wrap.close();
                    return;
                }

                try {
                    // [6] Finally, implement close sequence.
                    closeImpl();
                } finally {
                    writeLock.unlock();
                }
            } finally {
                readLock.unlock();
            }
        } catch (InterruptedException ex) {
            // Non-graceful close!
            Thread.currentThread().interrupt();
            wrap.close();
            return;
        }
    }

    @Override
    public boolean isOpen() {
        return wrap.isOpen();
    }

    /**
     * Implement the close procedure.
     * 
     * Precondition: read & write locks are acquired
     * 
     * Postcondition: state is CLOSED
     */
    private void closeImpl() throws IOException {
        synchronized (stateLock) {
            if (State.CLOSED == state) {
                return;
            }
            state = State.CLOSING;
        }
        try {
            // NOTE: unread data may be lost. However, we assume this is desired
            // since we are transitioning to closing:
            inAppBuffer.clear();
            if (outNetBuffer.hasRemaining()) {
                wrap.write(outNetBuffer);
            }
            engine.closeOutbound();
            try {
                while (!engine.isOutboundDone()) {
                    if (writeImpl(EMPTY, 0, 1) < 0) {
                        throw new ClosedChannelException();
                    }
                }
                while (!engine.isInboundDone()) {
                    if (readImpl(EMPTY[0]) < 0) {
                        throw new ClosedChannelException();
                    }
                }
                engine.closeInbound();
            } catch (ClosedChannelException ex) {
                // already closed, ignore.
            }
        } finally {
            try {
                // No matter what happens, we need to close the
                // wrapped channel:
                wrap.close();
            } finally {
                // ...and no matter what happens, we need to
                // indicate that we are in a CLOSED state:
                synchronized (stateLock) {
                    state = State.CLOSED;
                }
            }
        }
    }

    /**
     * Read plaintext by decrypting the underlying wrap'ed sockets encrypted bytes.
     * 
     * @param dst is the buffer to populate between position and limit.
     * @return the number of bytes populated or -1 to indicate end of stream,
     *     and the dst position will also be incremented appropriately.
     */
    @Override
    public int read(ByteBuffer dst) throws IOException {
        int result = 0;
        while (0 == result) {
            if (!handshake()) {
                return -1;
            }
            if (!dst.hasRemaining()) {
                return 0;
            }
            result = readImpl(dst);
        }
        return result;
    }

    /**
     * Precondition: handshake() was called, or this code was called
     *     by the handshake() implementation.
     */
    private int readImpl(ByteBuffer dst) throws IOException {
        readLock.lock();
        try {
            // [1] Check if this is a read for a handshake:
            synchronized (stateLock) {
                if (isHandshaking(state)) {
                    if (state != State.HANDSHAKING_READ) {
                        return 0;
                    }
                    dst = EMPTY[0];
                }
                readThread = Thread.currentThread();
            }

            // [2] Satisfy read via inAppBuffer:
            int count = append(inAppBuffer, dst);
            if (count > 0) {
                return count;
            }

            // [3] Read & decrypt loop:
            return readAndDecryptLoop(dst);
        } finally {
            readLock.unlock();
            readThread = null;
        }
    }

    /**
     * Return true if we are handshaking.
     */
    private static boolean isHandshaking(State state) {
        switch (state) {
        case HANDSHAKING_READ:
        case HANDSHAKING_WRITE:
        case HANDSHAKING_TASK:
            return true;

        case CLOSED:
        case OPEN:
        case CLOSING:
        }
        return false;
    }

    /**
     * Precondition: readLock acquired
     */
    private int readAndDecryptLoop(ByteBuffer dst) throws IOException {
        boolean networkRead = inNetBuffer.position() == 0;
        while (true) {
            // Read from network:
            if (networkRead) {
                synchronized (stateLock) {
                    if (State.OPEN == state && !dst.hasRemaining()) {
                        return 0;
                    }
                }
                if (wrap.read(inNetBuffer) < 0) {
                    return -1;
                }
            }

            SSLEngineResult result;
            synchronized(stateLock) {
                // Decrypt:
                inNetBuffer.flip();
                try {
                    result = engine.unwrap(inNetBuffer, dst);
                } finally {
                    inNetBuffer.compact();
                }
                State newState = toState(result.getHandshakeStatus());
                if (state != State.CLOSING && newState != state) {
                    state = newState;
                }
            }

            SSLEngineResult.Status status = result.getStatus();
            switch (status) {
            case BUFFER_OVERFLOW:
                if (dst == inAppBuffer) {
                    throw new IllegalStateException(
                        "SSLEngine indicated app buffer size=" + inAppBuffer.capacity() +
                        ", but unwrap() returned BUFFER_OVERFLOW with an empty buffer");
                }

                // Not enough space in dst, so buffer it into inAppBuffer:
                readAndDecryptLoop(inAppBuffer);

                return append(inAppBuffer, dst);

            case BUFFER_UNDERFLOW:
                if (!inNetBuffer.hasRemaining()) {
                    throw new IllegalStateException(
                        "SSLEngine indicated net buffer size=" + inNetBuffer.capacity() +
                        ", but unwrap() returned BUFFER_UNDERFLOW with a full buffer");
                }
                networkRead = inNetBuffer.hasRemaining();
                break; // retry network read

            case CLOSED:
                try {
                    wrap.close();
                } finally {
                    synchronized (stateLock) {
                        state = State.CLOSED;
                    }
                }
                return -1;

            case OK:
                return result.bytesProduced();

            default:
                throw new IllegalStateException("Unexpected status=" + status);
            }
        }
    }

    /**
     * Write plaintext by encrypting and writing this to the underlying wrap'ed socket.
     * 
     * @param srcs are the buffers of plaintext to encrypt.
     * @param offset is the offset within the array to begin writing.
     * @param length is the number of buffers within the srcs array that should be written.
     * @return the number of bytes that got written or -1 to indicate end of
     *     stream and the src position will also be incremented appropriately.
     */
    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        int result = 0;
        while (0 == result) {
            if (!handshake()) {
                return -1;
            }
            if (0 == remaining(srcs, offset, length)) {
                return 0;
            }
            result = writeImpl(srcs, offset, length);
        }
        return result;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return Math.toIntExact(write(new ByteBuffer[]{src}, 0, 1));
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return write(srcs, 0, srcs.length);
    }

    /**
     * While there are delegatedTasks to run, run them.
     * 
     * Precondition: stateLock acquired
     */
    private void executeTasks() {
        while (true) {
            Runnable runnable = engine.getDelegatedTask();
            if (null == runnable) {
                break;
            }
            runnable.run();
        }
    }

    /**
     * Implement a write operation.
     * 
     * @param src is the source buffer to write
     * @return the number of bytes written or -1 if end of stream.
     * 
     * Precondition: write lock is acquired.
     */
    private int writeImpl(ByteBuffer[] srcs, int offset, int length) throws IOException {
        writeLock.lock();
        try {
            // [1] Wait until handshake is complete in other thread.
            synchronized (stateLock) {
                if (isHandshaking(state)) {
                    if (state != State.HANDSHAKING_WRITE) {
                        return 0;
                    }
                    srcs = EMPTY;
                }
                writeThread = Thread.currentThread();
            }

            // [2] Write & decrypt loop:
            return writeAndEncryptLoop(srcs, offset, length);
        } finally {
            writeLock.unlock();
            writeThread = null;
        }
    }


    private int writeAndEncryptLoop(ByteBuffer[] srcs, int offset, int length) throws IOException {
        if (offset >= length) {
            return 0;
        }

        int count = 0;
        boolean finalNetFlush = false;
        int srcsEnd = offset + length;
        while (true) {
            SSLEngineResult result = null;
            synchronized (stateLock) {
                // Encrypt:
                outNetBuffer.compact();
                try {
                    for (; offset < srcsEnd; offset++, length--) {
                        ByteBuffer src = srcs[offset];
                        int startPosition = src.position();
                        result = engine.wrap(src, outNetBuffer);
                        count += src.position() - startPosition;
                        if (result.getStatus() != SSLEngineResult.Status.OK) {
                            break;
                        }
                    }
                } finally {
                    outNetBuffer.flip();
                }
                State newState = toState(result.getHandshakeStatus());
                if (state != State.CLOSING && state != newState) {
                    state = newState;
                }
            }

            SSLEngineResult.Status status = result.getStatus();
            switch (status) {
            case BUFFER_OVERFLOW:
                if (outNetBuffer.remaining() == outNetBuffer.capacity()) {
                    throw new IllegalStateException(
                        "SSLEngine indicated net buffer size=" + outNetBuffer.capacity() +
                        ", but wrap() returned BUFFER_OVERFLOW with a full buffer");
                }
                break; // retry network write.

            case BUFFER_UNDERFLOW:
                throw new IllegalStateException("SSLEngine.wrap() should never return BUFFER_UNDERFLOW");

            case CLOSED:
                finalNetFlush = true;
                break;

            case OK:
                finalNetFlush = offset >= srcsEnd;
                break; // perform a final net write.

            default:
                throw new IllegalStateException("Unexpected status=" + result.getStatus());
            }

            // Write to network:
            if (outNetBuffer.remaining() > 0) {
                if (wrap.write(outNetBuffer) < 0) {
                    return -1;
                }
            }
            if (finalNetFlush || 0 == remaining(srcs, offset, length)) {
                break;
            }
        }
        return count;
    }
}