package io.nats.client.impl;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

import static java.nio.charset.StandardCharsets.US_ASCII;

public class ByteArrayBuilder {

    private final Charset defaultCharset;
    private ByteBuffer buffer;

    public ByteArrayBuilder() {
        this(256, US_ASCII);
    }

    public ByteArrayBuilder(int initialSize) {
        this(initialSize, US_ASCII);
    }

    public ByteArrayBuilder(int initialSize, Charset defaultCharset) {
        this.buffer = ByteBuffer.allocate(initialSize);
        this.defaultCharset = defaultCharset;
    }

    public byte[] toByteArray() {
        return Arrays.copyOf(buffer.array(), buffer.position());
    }

    protected int computeNewAllocationSize(int currentCapacity, int currentPosition, int bytesNeeded) {
        return Math.max(currentCapacity * 2, currentPosition + bytesNeeded);
    }

    private void ensureCapacity(int bytesNeeded) {
        if (buffer.capacity() - buffer.position() < bytesNeeded) {
            ByteBuffer newBuffer
                    = ByteBuffer.allocate(
                            computeNewAllocationSize(buffer.capacity(), buffer.position(), bytesNeeded));
            newBuffer.put(buffer.array(), 0, buffer.position());
            buffer = newBuffer;
        }
    }

    public ByteArrayBuilder append(Integer i) {
        append(i.toString().getBytes(US_ASCII)); // a number is always ascii
        return this;
    }

    public ByteArrayBuilder append(String s) {
        append(s.getBytes(defaultCharset));
        return this;
    }

    public ByteArrayBuilder append(String s, Charset charset) {
        append(s.getBytes(charset));
        return this;
    }

    public ByteArrayBuilder append(byte[] bytes) {
        ensureCapacity(bytes.length);
        buffer.put(bytes, 0, bytes.length);
        return this;
    }

    public ByteArrayBuilder append(byte[] bytes, int len) {
        ensureCapacity(len);
        buffer.put(bytes, 0, len);
        return this;
    }

    public ByteArrayBuilder append(byte[] bytes, int off, int len) {
        ensureCapacity(len);
        buffer.put(bytes, off, len);
        return this;
    }
}
