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
        this.buffer = ByteBuffer.allocate(computeNewAllocationSize(0, initialSize));
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

    /**
     * Append a space
     *
     * @return this (fluent)
     */
    public ByteArrayBuilder appendSpace() {
        return append(SPACE, 0, 1);
    }

    /**
     * Append the two characters for CR / LF
     *
     * @return this (fluent)
     */
    public ByteArrayBuilder appendCrLf() {
        return append(CRLF, 0, 2);
    }

    /**
     * Append a String representation of the number.
     *
     * @param  i the number
     * @return this (fluent)
     */
    public ByteArrayBuilder append(int i) {
        append(Integer.toString(i).getBytes(US_ASCII)); // a number is always ascii
        return this;
    }

    /**
     * Append a String with the default charset.
     * If the src is null, the word 'null' is appended.
     *
     * @param  src
     *         The String from which bytes are to be read
     * @return this (fluent)
     */
    public ByteArrayBuilder append(String src) {
        return append(src, defaultCharset);
    }

    /**
     * Append a String with specified charset.
     * If the src is null, the word 'null' is appended.
     *
     * @param  src
     *         The String from which bytes are to be read
     * @param charset the charset for encoding
     * @return this (fluent)
     */
    public ByteArrayBuilder append(String src, Charset charset) {
        return src == null ? append(NULL, 0, 4) : append(src.getBytes(charset));
    }

    /**
     * Append a CharBuffer with default charset.
     * If the src is null, the word 'null' is appended.
     *
     * @param  src
     *         The CharBuffer from which bytes are to be read
     * @return this (fluent)
     */
    public ByteArrayBuilder append(CharBuffer src) {
        return append(src, defaultCharset);
    }

    /**
     * Append a CharBuffer with specified charset.
     * If the src is null, the word 'null' is appended.
     *
     * @param  src
     *         The CharBuffer from which bytes are to be read
     * @param charset the charset for encoding
     * @return this (fluent)
     */
    public ByteArrayBuilder append(CharBuffer src, Charset charset) {
        if (src == null) {
            append(NULL, 0, 4);
        }
        else {
            append(src.toString().getBytes(charset));
        }
        return this;
    }

    /**
     * Append a byte as is
     *
     * @param  b the byte
     * @return this (fluent)
     */
    public ByteArrayBuilder append(byte b) {
        ensureCapacity(1);
        buffer.put(b);
        return this;
    }

    /**
     * Append a byte array
     *
     * @param  src
     *         The array from which bytes are to be read
     * @return this (fluent)
     */
    public ByteArrayBuilder append(byte[] src) {
        if (src.length > 0) {
            ensureCapacity(src.length);
            buffer.put(src, 0, src.length);
        }
        return this;
    }

    /**
     * Append a byte array
     *
     * @param  src
     *         The array from which bytes are to be read
     * @param  len
     *         The number of bytes to be read from the given array;
     *         must be non-negative and no larger than
     *         <tt>array.length - offset</tt>
     * @return this (fluent)
     */
    public ByteArrayBuilder append(byte[] src, int len) {
        if (len > 0) {
            ensureCapacity(len);
            buffer.put(src, 0, len);
        }
        return this;
    }

    /**
     * Append a byte array
     *
     * @param  src
     *         The array from which bytes are to be read
     * @param  offset
     *         The offset within the array of the first byte to be read;
     *         must be non-negative and no larger than <tt>array.length</tt>
     * @param  len
     *         The number of bytes to be read from the given array;
     *         must be non-negative and no larger than
     *         <tt>array.length - offset</tt>
     * @return this (fluent)
     */
    public ByteArrayBuilder append(byte[] src, int offset, int len) {
        if (len > 0) {
            ensureCapacity(len);
            buffer.put(src, offset, len);
        }
        return this;
    }

    @Override
    public String toString() {
        return new String(toByteArray(), defaultCharset);
    }
}
