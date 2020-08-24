package io.nats.client.impl;

import java.nio.ByteBuffer;

public class ByteBufferUtil {

    public static ByteBuffer enlargeBuffer(ByteBuffer buffer, int atLeast) {
        int current = buffer.capacity();
        int newSize = Math.max(current * 2, atLeast);
        ByteBuffer newBuffer = ByteBuffer.allocate(newSize);
        buffer.flip();
        newBuffer.put(buffer);
        return newBuffer;
    }
}
