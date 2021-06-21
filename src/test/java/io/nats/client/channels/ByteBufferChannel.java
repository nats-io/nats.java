package io.nats.client.channels;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.GatheringByteChannel;

import static io.nats.client.support.BufferUtils.append;

/**
 * Simple class that buffers all reads into an internal ByteBuffer and satisifes
 * all writes via the same ByteBuffer. Really only intended for tests.
 */
public class ByteBufferChannel implements ByteChannel, GatheringByteChannel {
    private ByteBuffer buffer;
    private boolean closed = false;

    public ByteBufferChannel(ByteBuffer initialBuffer) {
        assert null != initialBuffer;
        this.buffer = initialBuffer;
    }

    /**
     * Access the internal ByteBuffer, but note that it may change if
     * write() calls require the space to be expanded.
     * 
     * NOTE: This buffer is ALWAYS in "put" mode.
     */
    public ByteBuffer getByteBuffer() {
        return buffer;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        if (closed) {
            throw new ClosedChannelException();
        }
        int result = append(buffer, dst);
        // Translate 0 into end of stream:
        return result == 0 ? -1 : result;
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    @Override
    public void close() throws IOException {
        closed = true;
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        if (closed) {
            throw new ClosedChannelException();
        }
        long size = 0;
        int endOffset = offset + length;
        while (offset < endOffset) {
            ByteBuffer src = srcs[offset++];
            ensureRemaining(src.remaining());
            int position = buffer.position();
            buffer.put(src);
            size += buffer.position() - position;
        }
        return size;
    }

    private void ensureRemaining(int remaining) {
        if (buffer.remaining() >= remaining) {
            return;
        }
        ByteBuffer newBuffer = ByteBuffer.allocate(buffer.position() + remaining);
        buffer.flip();
        newBuffer.put(buffer);
        buffer = newBuffer;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return Math.toIntExact(write(new ByteBuffer[]{src}, 0, 1));
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return write(srcs, 0, srcs.length);
    }
}
