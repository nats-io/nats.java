package io.nats.client.support;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;

/**
 * It blows my mind that JDK doesn't provide this functionality by default.
 * 
 * This is an implementation of ByteChannel which uses an SSLEngine to encrypt data sent
 * to a ByteChannel that is being wrapped, and then decrypts received data.
 */
public class TLSByteChannel implements ByteChannel {
    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

    // NOTE: Care should be taken to ensure readLock is acquired BEFORE writeLock
    // in the case where operations perform on both.
    private final Lock readLock = new ReentrantLock();
    private final Lock writeLock = new ReentrantLock();

    private final ByteChannel wrap;
    private final SSLEngine engine;

    private final ByteBuffer outNetBuffer; // ready for read

    private final ByteBuffer inNetBuffer; // ready for write
    private final ByteBuffer inAppBuffer; // ready for read

    private State state = State.HANDSHAKING;
    private Thread readThread = null;
    private Thread writeThread = null;

    private enum State {
        HANDSHAKING,
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

        inNetBuffer = ByteBuffer.allocate(netBufferSize);
        inNetBuffer.limit(0);
        inAppBuffer = ByteBuffer.allocate(appBufferSize);

        engine.beginHandshake();
    }

    /**
     * Gracefully close the TLS session and the underlying wrap'ed socket.
     */
    @Override
    public void close() throws IOException {
        readLock.lock();
        if (null != readThread) {
            readThread.interrupt();
        }
        try {
            writeLock.lock();
            if (null != writeThread) {
                writeThread.interrupt();
            }
            try {
                closeImpl();
            } finally {
                writeLock.unlock();
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        if (state == State.CLOSED) {
            return -1;
        }
        maybeHandshake();
        readLock.lock();
        try {
            return readImpl(dst, true);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        maybeHandshake();
        writeLock.lock();
        try {
            return writeImpl(src, true);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean isOpen() {
        switch (state) {
        case HANDSHAKING:
        case OPEN:
            return true;
        default:
            return false;
        }
    }

    /**
     * Checks the state to see if we need to handshake. If so,
     * aquire the read & write locks and perform handshake.
     */
    private void maybeHandshake() throws IOException {
        if (State.HANDSHAKING != state) {
            return;
        }
        readLock.lock();
        try {
            writeLock.lock();
            try {
                handshakeImpl(true);
            } finally {
                writeLock.unlock();
            }
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Perform the handshake loop, if state change is true,
     * the post condition will transition the state to OPEN.
     */
    private void handshakeImpl(boolean stateChange) throws IOException {
        while (true) {
            switch (engine.getHandshakeStatus()) {
            case NEED_TASK:
                executeTasks();
                break;

            case NEED_UNWRAP:
                if ( readImpl(inAppBuffer, false) < 0 ) {
                    return;
                }
                break;

            case NEED_WRAP:
                writeImpl(EMPTY, false);
                break;

            case FINISHED:
            case NOT_HANDSHAKING:
                if (stateChange) {
                    state = State.OPEN;
                }
                return;
            default:
                throw new IllegalStateException("Unexpected SSLEngine.HandshakeStatus=" + engine.getHandshakeStatus());
            }
        }
    }

    /**
     * While there are delegatedTasks to run, run them.
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
     * Implement close(), assumes all locks have been aquired.
     */
    private void closeImpl() throws IOException {
        switch (state) {
        case HANDSHAKING:
            // Finish the handshake, then close:
            state = State.CLOSING;
            handshakeImpl(false);
            // intentionally, no break!
        case OPEN:
            transitionToClosed();
        default:
        }
    }

    /**
     * Implement the transition from CLOSING to CLOSED state.
     */
    private void transitionToClosed() throws IOException {
        state = State.CLOSING;
        try {
            // NOTE: unread data may be lost. However, we assume this is desired
            // since we are transitioning to closing:
            inAppBuffer.clear();
            // This is the close sequence recommended by SSLEngine docs:
            engine.closeOutbound();
            writeImpl(EMPTY, false);
            readImpl(inAppBuffer, false);
            engine.closeInbound();
        } finally {
            try {
                // No matter what happens, we need to close the
                // wrapped channel:
                wrap.close();
            } finally {
                // ...and no matter what happens, we need to
                // indicate that we are in a CLOSED state:
                state = State.CLOSED;
            }
        }
    }

    /**
     * Implement a read into dst buffer, potientially transitioning to
     * HANDSHAKING or CLOSED state only if stateChange is true.
     * 
     * @param dst is the buffer to write into, if it is empty, an attempt to read
     *     the wrap'ed channel will still be made and this network data will still
     *     be propegated to the SSLEngine. This is so this method can be used in
     *     the handshake process.
     * 
     * @param stateChange should be set to true if transitioning into HANDSHAKING
     *     state is okay.
     * 
     * @return the number of bytes written into the dst buffer.
     */
    private int readImpl(ByteBuffer dst, boolean stateChange) throws IOException {
        int count = 0;
        if (dst != inAppBuffer) {
            count += flushInAppBuffer(dst);
            if (exhausted(inAppBuffer)) {
                return 0;
            }
        }

        if (!inNetBuffer.hasRemaining()) {
            inNetBuffer.limit(0);
            int netCount = tryNetRead(stateChange);
            if (netCount < 1) {
                return count > 0 ? count : netCount;
            }
        }
        SSLEngineResult result = engine.unwrap(inNetBuffer, dst);
        if (stateChange && isHandshaking(result.getHandshakeStatus())) {
            state = State.HANDSHAKING;
        }
        switch (result.getStatus()) {
        case BUFFER_OVERFLOW:
            if (dst == inAppBuffer) {
                throw new IllegalStateException("SSLEngine.unwrap() returned unexpected BUFFER_OVERFLOW");
            }
            // Not enough space in dst, so buffer it into inAppBuffer:
            readImpl(inAppBuffer, stateChange);
            // ...and flush the inAppBuffer into dst:
            return count + flushInAppBuffer(dst);

        case BUFFER_UNDERFLOW:
            // tryNetRead() didn't return enough, so just return count:
            return count;

        case CLOSED:
            if (stateChange) {
                try {
                    wrap.close();
                } finally {
                    state = State.CLOSED;
                }
            }
            return count > 0 ? count : -1;

        case OK:
            return count + result.bytesProduced();

        default:
            throw new IllegalStateException("Unexpected status=" + result.getStatus());
        }
    }

    private int flushInAppBuffer(ByteBuffer dst) {
        if (inAppBuffer.position() <= 0) {
            return 0;
        }
        inAppBuffer.flip();
        safePut(dst, inAppBuffer);
        int count = inAppBuffer.position();
        inAppBuffer.compact();
        return count;
    }

    private int tryNetRead(boolean interruptable) throws IOException {
        if (inNetBuffer.limit() >= inNetBuffer.capacity()) {
            // No capacity to read any more.
            return 0;
        }
        prepareForAppend(inNetBuffer);
        int result;
        if (interruptable) {
            readThread = Thread.currentThread();
            readLock.unlock();
            try {
                result = wrap.read(inNetBuffer);
            } finally {
                readLock.lock();
                readThread = null;
            }
        } else {
            result = wrap.read(inNetBuffer);
        }
        inNetBuffer.flip();
        return result;
    }

    /**
     * Implement a write operation.
     * 
     * @param src is the source buffer to write
     */
    private int writeImpl(ByteBuffer src, boolean stateChange) throws IOException {
        if (!flushNetWrite(stateChange)) {
            // Still waiting for the wrapped byte channel to consume outNetBuffer.
            return 0;
        }
        int count = 0;

        do {
            int position = src.position();
            SSLEngineResult result = engine.wrap(src, outNetBuffer);
            count += src.position() - position;

            if (stateChange && isHandshaking(result.getHandshakeStatus())) {
                state = State.HANDSHAKING;
            }

            switch (result.getStatus()) {
            case BUFFER_OVERFLOW:
                if (!flushNetWrite(stateChange)) {
                    return count;
                }
                break;
            case BUFFER_UNDERFLOW:
                throw new IllegalStateException("SSLEngine.wrap() should never return BUFFER_UNDERFLOW");
            case CLOSED:
                if (stateChange) {
                    try {
                        wrap.close();
                    } finally {
                        state = State.CLOSED;
                    }
                }
                return count;
            case OK:
                flushNetWrite(stateChange);
                return count;

            default:
                throw new IllegalStateException("Unexpected status=" + result.getStatus());
            }
        } while (src.hasRemaining());
        return count;
    }

    /**
     * Write the outNetBuffer to the wrapped ByteChannel.
     * 
     * @return false if no capacity remains in the outNetBuffer
     */
    private boolean flushNetWrite(boolean interruptable) throws IOException {
        if (outNetBuffer.position() > 0) {
            outNetBuffer.flip();
            try {
                if (interruptable) {


                    writeThread = Thread.currentThread();
                    writeLock.unlock();
                    try {
                        wrap.write(outNetBuffer);
                    } finally {
                        writeLock.lock();
                        writeThread = null;
                    }
                } else {
                    wrap.write(outNetBuffer);
                }
            } finally {
                outNetBuffer.compact();
            }
            if (exhausted(outNetBuffer)) {
                // Must not have written anything, and the net buffer is full:
                return false;
            }
        }
        return true;
    }

    /**
     * @param status is the handshake status to test.
     * @return true if HandshakeStatus indicates that a handshake is still in progress.
     */
    private static boolean isHandshaking(HandshakeStatus status) {
        switch (status) {
        case NOT_HANDSHAKING:
        case FINISHED:
            return false;
        default:
            return true;
        }
    }

    /**
     * Acts like {@link ByteBuffer.put(ByteBuffer)} but puts as many bytes
     * as possible into dst and returns false if a BufferOverflowException
     * would occur.
     * 
     * @param src is the source buffer to read from.
     * @param dst is the destination buffer to put into the source.
     * @return true if a BufferOverflowException would have been triggered.
     */
    private static boolean safePut(ByteBuffer src, ByteBuffer dst) {
        if (src.remaining() <= dst.remaining()) {
            dst.put(src);
            return true;
        }
        int limit = src.limit();
        src.limit(src.position() + dst.remaining());
        dst.put(src);
        src.limit(limit);
        return false;
    }

    /**
     * Sets position = limit and limit = capacity. You can kind of think of it
     * as the opposite of flip().
     * 
     * @param buff is the buffer to prepare for an append.
     */
    private static void prepareForAppend(ByteBuffer buff) {
        buff.position(buff.limit());
        buff.limit(buff.capacity());
    }

    /**
     * @param buff the buffer to test
     * @return true if position == capacity, basically there is no more space
     *     to put() anything else in the buffer.
     */
    private static boolean exhausted(ByteBuffer buff) {
        return buff.position() == buff.capacity();
    }
}
