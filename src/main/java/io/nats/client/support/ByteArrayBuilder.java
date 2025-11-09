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

package io.nats.client.support;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

import static java.nio.charset.StandardCharsets.ISO_8859_1;

/**
 * A class that wraps a ByteBuffer that can automatically grow
 */
public class ByteArrayBuilder extends BuilderBase {
    private ByteBuffer buffer;

    /**
     * Construct the ByteArrayBuilder with the supplied initial size,
     * allocation size and character set
     * @param initialSize the initial size
     * @param allocationSizeSuggestion the allocationSize size suggestion
     * @param defaultCharset the default character set
     */
    public ByteArrayBuilder(int initialSize, int allocationSizeSuggestion, Charset defaultCharset) {
        super(defaultCharset, allocationSizeSuggestion);
        this.buffer = ByteBuffer.allocate(bufferAllocSize(initialSize, allocationSize));
    }

    /**
     * Construct the ByteArrayBuilder with
     * the initial size and allocation size of {@value #DEFAULT_ASCII_ALLOCATION}
     * and the character set {@link java.nio.charset.StandardCharsets#ISO_8859_1}
     * since ISO_8859_1 is faster when encoding and decoding than ISO_8859_1
     */
    public ByteArrayBuilder() {
        this(-1, DEFAULT_ASCII_ALLOCATION, ISO_8859_1);
    }

    /**
     * Construct the ByteArrayBuilder with the supplied initial size,
     * allocation size of {@value #DEFAULT_ASCII_ALLOCATION}
     * and the character set {@link java.nio.charset.StandardCharsets#ISO_8859_1}
     *
     * @param initialSize the initial size
     */
    public ByteArrayBuilder(int initialSize) {
        this(initialSize, DEFAULT_ASCII_ALLOCATION, ISO_8859_1);
    }

    /**
     * Construct the ByteArrayBuilder with the supplied character set
     * with the default initial size and allocation size determined by that character set
     *
     * @param defaultCharset the default character set
     */
    public ByteArrayBuilder(Charset defaultCharset) {
        this(-1, -1, defaultCharset);
    }

    /**
     * Construct the ByteArrayBuilder with the supplied initial size and character set
     * with the allocation size determined by that character set.
     *
     * @param initialSize the initial size
     * @param defaultCharset the default character set
     */
    public ByteArrayBuilder(int initialSize, Charset defaultCharset) {
        this(initialSize, -1, defaultCharset);
    }

    /**
     * Construct the ByteArrayBuilder copying all the bytes
     * using the character set {@link java.nio.charset.StandardCharsets#ISO_8859_1}
     * and the character set {@link java.nio.charset.StandardCharsets#ISO_8859_1}
     * Then initializes the buffer with the supplied bytes
     * @param bytes the bytes
     */
    public ByteArrayBuilder(byte[] bytes) {
        this(bytes, bytes.length);
    }

    /**
     * Construct the ByteArrayBuilder copying the specified number of bytes;
     * and the character set {@link java.nio.charset.StandardCharsets#ISO_8859_1}
     * Then initializes the buffer with the supplied bytes
     * @param bytes the bytes
     * @param len the number of bytes to copy
     */
    public ByteArrayBuilder(byte[] bytes, int len) {
        super(ISO_8859_1, DEFAULT_ASCII_ALLOCATION);
        this.buffer = ByteBuffer.allocate(bufferAllocSize(len, allocationSize));
        buffer.put(bytes, 0, bytes.length);
    }

    /**
     * Get the length of the data in the buffer
     * @return the length of the data
     */
    @Override
    public int length() {
        return buffer.position();
    }

    /**
     * Get the number of bytes currently allocated (available) without resizing
     * @return the number of bytes
     */
    @Override
    public int capacity() {
        return buffer.capacity();
    }

    /**
     * Determine if a byte array contains the same bytes as this builder
     * @param bytes the bytes
     * @return true if the supplied value equals what is in the builder
     */
    @Override
    public boolean equals(byte[] bytes) {
        if (bytes == null || buffer.position() != bytes.length) {
            return false;
        }
        byte[] hb = buffer.array();
        for (int x = 0; x < bytes.length; x++) {
            if (hb[x] != bytes[x]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Copy the contents of the buffer to the byte array starting at the destination
     * positions supplied. Assumes that the {@link #length} method has been called
     * and the destination byte array has enough space allocated
     * @param dest the destination byte array
     * @param destPos the starting position in the destination byte array
     * @return the number of bytes copied
     */
    public int copyTo(byte[] dest, int destPos) {
        int len = length();
        byte[] hb = buffer.array();
        System.arraycopy(hb, 0, dest, destPos, len);
        return len;
    }

    /**
     * Copy the contents of the buffer to the output stream
     * @param out the output stream
     * @throws IOException if an I/O error occurs
     */
    public void copyTo(OutputStream out) throws IOException {
        out.write(buffer.array(), 0, buffer.position());
    }

    /**
     * Copy the value in the buffer to a new byte array
     * @return the copy of the bytes
     */
    @Override
    public byte[] toByteArray() {
        return Arrays.copyOf(buffer.array(), buffer.position());
    }

    /**
     * Access the internal byte array of this buffer. Intended for read only
     * with knowledge of {@link #length}
     *
     * @return a direct handle to the internal byte array
     */
    @Override
    public byte[] internalArray() {
        return buffer.array();
    }

    /**
     * Ensures that the buffer can accept the number of bytes needed
     * Useful if the size of multiple append operations is known ahead of time
     * @param bytesNeeded the number of bytes needed
     * @return this (fluent)
     */
    public ByteArrayBuilder ensureCapacity(int bytesNeeded) {
        int bytesAvailable = buffer.capacity() - buffer.position();
        if (bytesAvailable < bytesNeeded) {
            ByteBuffer newBuffer = ByteBuffer.allocate(
                bufferAllocSize(buffer.position() + bytesNeeded, allocationSize));
            newBuffer.put(buffer.array(), 0, buffer.position());
            buffer = newBuffer;
        }
        return this;
    }

    /**
     * Clear the buffer, resetting its length but not capacity
     * @return this (fluent)
     */
    public ByteArrayBuilder clear() {
        buffer.clear();
        return this;
    }

    /**
     * Change the allocation size
     * @param allocationSizeSuggestion the new allocation size suggestion
     * @return this (fluent)
     */
    public ByteArrayBuilder setAllocationSize(int allocationSizeSuggestion) {
        _setAllocationSize(allocationSizeSuggestion);
        return this;
    }

    /**
     * Append a String representation of the number.
     * @param  i the number
     * @return this (fluent)
     */
    public ByteArrayBuilder append(int i) {
        append(Integer.toString(i).getBytes(ISO_8859_1)); // a number is always ascii (ISO_8859_1 is faster)
        return this;
    }

    /**
     * Append a String with the default charset.
     * If the src is null, the word 'null' is appended.
     * @param src The String from which bytes are to be read
     * @return this (fluent)
     */
    public ByteArrayBuilder append(String src) {
        return append(src, defaultCharset);
    }

    /**
     * Append a String with specified charset.
     * If the src is null, the word 'null' is appended.
     * @param src The String from which bytes are to be read
     * @param charset the charset for encoding
     * @return this (fluent)
     */
    public ByteArrayBuilder append(String src, Charset charset) {
        return src == null ? append(NULL, 0, 4) : append(src.getBytes(charset));
    }

    /**
     * Append a CharBuffer with default charset.
     * If the src is null, the word 'null' is appended.
     * @param src The CharBuffer from which bytes are to be read
     * @return this (fluent)
     */
    public ByteArrayBuilder append(CharBuffer src) {
        return append(src, defaultCharset);
    }

    /**
     * Append a CharBuffer with specified charset.
     * If the src is null, the word 'null' is appended.
     * @param src The CharBuffer from which bytes are to be read
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
     * @param b the byte
     * @return this (fluent)
     */
    public ByteArrayBuilder append(byte b) {
        ensureCapacity(1);
        buffer.put(b);
        return this;
    }

    /**
     * Append an entire byte array
     * @param src The array from which bytes are to be read
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
     * @param src The array from which bytes are to be read
     * @param  len The number of bytes to be read from the given array
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
     * @param src The array from which bytes are to be read
     * @param  offset The offset within the array of the first byte to be read;
     * @param  len The number of bytes to be read from the given array;
     * @return this (fluent)
     */
    public ByteArrayBuilder append(byte[] src, int offset, int len) {
        if (len > 0) {
            ensureCapacity(len);
            buffer.put(src, offset, len);
        }
        return this;
    }

    /**
     * Appends the data bytes from an existing byte array builder
     * @param bab an existing builder
     * @return this (fluent)
     */
    public ByteArrayBuilder append(ByteArrayBuilder bab) {
        if (bab != null && bab.length() > 0) {
            append(bab.buffer.array(), 0, bab.length());
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int appendUnchecked(byte b) {
        buffer.put(b);
        return 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int appendUnchecked(byte[] src) {
        buffer.put(src, 0, src.length);
        return src.length;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int appendUnchecked(byte[] src, int srcPos, int len) {
        buffer.put(src, srcPos, len);
        return len;
    }

    @Override
    public String toString() {
        return new String(buffer.array(), 0, buffer.position(), defaultCharset);
    }
}
