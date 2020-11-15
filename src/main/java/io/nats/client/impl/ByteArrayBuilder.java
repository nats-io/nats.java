// Copyright 2015-2020 The NATS Authors
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
    private static byte[] NULL = "null".getBytes(US_ASCII);
    private static byte[] SPACE = " ".getBytes(US_ASCII);
    private static byte[] CRLF = "\r\n".getBytes(US_ASCII);

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
