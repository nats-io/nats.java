// Copyright 2020 The NATS Authors
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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

import static java.nio.charset.StandardCharsets.US_ASCII;

public class ByteArrayBuilder {
    public static final int DEFAULT_ASCII_ALLOCATION = 32;
    public static final int DEFAULT_OTHER_ALLOCATION = 64;
    private static final byte[] NULL = "null".getBytes(US_ASCII);
    private static final byte[] SPACE = " ".getBytes(US_ASCII);
    private static final byte[] CRLF = "\r\n".getBytes(US_ASCII);

    private final int allocationSize;
    private final Charset defaultCharset;
    private ByteBuffer buffer;


    public ByteArrayBuilder() {
        this(DEFAULT_ASCII_ALLOCATION, US_ASCII);
    }

    public ByteArrayBuilder(int initialSize) {
        this(initialSize, US_ASCII);
    }

    public ByteArrayBuilder(Charset defaultCharset) {
        this(0, defaultCharset);
    }

    public ByteArrayBuilder(int initialSize, Charset defaultCharset) {
        allocationSize = defaultCharset == US_ASCII ? DEFAULT_ASCII_ALLOCATION : DEFAULT_OTHER_ALLOCATION;
        this.buffer = ByteBuffer.allocate(computeNewAllocationSize(0, Math.max(allocationSize, initialSize)));
        this.defaultCharset = defaultCharset;
    }

    public byte[] toByteArray() {
        return Arrays.copyOf(buffer.array(), buffer.position());
    }

    protected int computeNewAllocationSize(int currentPosition, int bytesNeeded) {
        return ((currentPosition + bytesNeeded + allocationSize) / allocationSize) * allocationSize;
    }

    private void ensureCapacity(int bytesNeeded) {
        int bytesAvailable = buffer.capacity() - buffer.position();
        if (bytesAvailable < bytesNeeded) {
            ByteBuffer newBuffer
                    = ByteBuffer.allocate(
                            computeNewAllocationSize(buffer.position(), bytesNeeded));
            newBuffer.put(buffer.array(), 0, buffer.position());
            buffer = newBuffer;
        }
    }

    public ByteArrayBuilder appendSpace() {
        return append(SPACE, 0, 1);
    }

    public ByteArrayBuilder appendCrLf() {
        return append(CRLF, 0, 2);
    }

    public ByteArrayBuilder append(int i) {
        append(Integer.toString(i).getBytes(US_ASCII)); // a number is always ascii
        return this;
    }

    public ByteArrayBuilder append(String s) {
        return s == null ? append(NULL, 0, 4) : append(s.getBytes(defaultCharset));
    }

    public ByteArrayBuilder append(String s, Charset charset) {
        return s == null ? append(NULL, 0, 4) : append(s.getBytes(charset));
    }

    public ByteArrayBuilder append(CharBuffer cb) {
        append(cb.toString().getBytes(defaultCharset));
        return this;
    }

    public ByteArrayBuilder append(CharBuffer cb, Charset charset) {
        append(cb.toString().getBytes(charset));
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
