// Copyright 2022 The NATS Authors
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
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

import static java.nio.charset.StandardCharsets.US_ASCII;

public class ByteArrayPrimitiveBuilder {
    public static final ByteArrayPrimitiveBuilder EMPTY_BAPB = new ByteArrayPrimitiveBuilder();

    public static final int DEFAULT_ASCII_ALLOCATION = 32;
    public static final int DEFAULT_OTHER_ALLOCATION = 64;
    public static final byte[] NULL = "null".getBytes(US_ASCII);

    private final Charset defaultCharset;
    private byte[] buffer;
    private int allocationSize;
    private int position;

    /**
     * Construct the ByteArrayPrimitiveBuilder with
     * the initial size and allocation size of {@value #DEFAULT_ASCII_ALLOCATION}
     * and the character set {@link java.nio.charset.StandardCharsets#US_ASCII}
     */
    public ByteArrayPrimitiveBuilder() {
        this(DEFAULT_ASCII_ALLOCATION, DEFAULT_ASCII_ALLOCATION, US_ASCII);
    }

    /**
     * Construct the ByteArrayPrimitiveBuilder with the supplied initial size,
     * allocation size of {@value #DEFAULT_ASCII_ALLOCATION}
     * and the character set {@link java.nio.charset.StandardCharsets#US_ASCII}
     * @param initialSize the initial size
     */
    public ByteArrayPrimitiveBuilder(int initialSize) {
        this(initialSize, DEFAULT_ASCII_ALLOCATION, US_ASCII);
    }

    /**
     * Construct the ByteArrayPrimitiveBuilder copying all the bytes in the src
     * and the character set {@link java.nio.charset.StandardCharsets#US_ASCII}
     * Then initializes the buffer with the supplied bytes
     * @param src the bytes
     */
    public ByteArrayPrimitiveBuilder(byte[] src) {
        this(src, src.length);
    }

    /**
     * Construct the ByteArrayPrimitiveBuilder copying the specfied number of bytes;
     * and the character set {@link java.nio.charset.StandardCharsets#US_ASCII}
     * Then initializes the buffer with the supplied bytes
     * @param src the bytes
     * @param len the number of bytes to copy
     */
    public ByteArrayPrimitiveBuilder(byte[] src, int len) {
        defaultCharset = US_ASCII;
        allocationSize = DEFAULT_ASCII_ALLOCATION;
        position = len;
        buffer = new byte[calculateArraySize(len)];
        System.arraycopy(src, 0, buffer, 0, len);
    }

    /**
     * Construct the ByteArrayPrimitiveBuilder with the supplied initial size,
     * allocation size and character set
     * @param initialSize the initial size
     * @param allocationSize the allocationSize size
     * @param defaultCharset the default character set
     */
    public ByteArrayPrimitiveBuilder(int initialSize, int allocationSize, Charset defaultCharset) {
        this.defaultCharset = defaultCharset;
        setAllocationSize(allocationSize);
        this.buffer = new byte[calculateArraySize(initialSize)];
        position = 0;
    }

    /**
     * Construct the ByteArrayPrimitiveBuilder with the supplied character set
     * with the default initial size and allocation size determined by that character set
     * @param defaultCharset the default character set
     */
    public ByteArrayPrimitiveBuilder(Charset defaultCharset) {
        this(-1, -1, defaultCharset);
    }

    /**
     * Construct the ByteArrayPrimitiveBuilder with the supplied initial size and character set
     * with the allocation size determined by that character set.
     * @param initialSize the initial size
     * @param defaultCharset the default character set
     */
    public ByteArrayPrimitiveBuilder(int initialSize, Charset defaultCharset) {
        this(initialSize, -1, defaultCharset);
    }

    /**
     * Get the length of the data in the buffer
     * @return the length of the data
     */
    public int length() {
        return position;
    }

    /**
     * Get the number of bytes currently allocated (available) without resizing
     * @return the number of bytes
     */
    public int capacity() {
        return buffer.length;
    }

    /**
     * Determine if a byte array contains the same bytes as this buffer
     * @param bytes the bytes
     * @return true if the supplied value equals what is in the buffer
     */
    public boolean equals(byte[] bytes) {
        if (bytes == null || position != bytes.length) {
            return false;
        }
        for (int x = 0; x < bytes.length; x++) {
            if (buffer[x] != bytes[x]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Copy the contents of the buffer to the byte array starting at the destination
     * positions supplied. Assumes that the {@link #position} method has been called
     * and the destination byte array has enough space allocated
     * @param dest the destination byte array
     * @param destPos the starting position in the destination byte array
     * @return the number of bytes copied
     */
    public int copyTo(byte[] dest, int destPos) {
        System.arraycopy(buffer, 0, dest, destPos, position);
        return position;
    }

    public void copyTo(OutputStream out) throws IOException {
        out.write(buffer, 0, position);
    }

    /**
     * Copy the value in the buffer to a new byte array
     * @return the copy of the bytes
     */
    public byte[] toByteArray() {
        return Arrays.copyOf(buffer, position);
    }

    /**
     * Access the internal byte array of this buffer. Intended for read only
     * with knowledge of {@link #length()}
     * @return a direct handle to the internal byte array
     */
    public byte[] internalArray() {
        return buffer;
    }

    /**
     * Ensures that the buffer can accept the number of bytes needed by resizing if necessary.
     * Useful if the size of multiple append operations is known ahead of time,
     * reducing the number of resizes
     * @param bytesNeeded the number of bytes needed
     * @return this (fluent)
     */
    public ByteArrayPrimitiveBuilder ensureCapacity(int bytesNeeded) {
        int bytesAvailable = buffer.length - position;
        if (bytesAvailable < bytesNeeded) {
            byte[] newBuffer = new byte[calculateArraySize(position + bytesNeeded)];
            System.arraycopy(buffer, 0, newBuffer, 0, position);
            buffer = newBuffer;
        }
        return this;
    }

    public static void main(String[] args) {
        ByteArrayPrimitiveBuilder b = new ByteArrayPrimitiveBuilder();
        System.out.println(b.allocationSize);
        for (int x = 1; x <= b.allocationSize * 2; x++) {
            System.out.println(x + " -> " + b.calculateArraySize(x));
        }
    }

    private int calculateArraySize(int bytes) {
        if (bytes < allocationSize) {
            return allocationSize;
        }
        int blocks = bytes / allocationSize;
        int size = blocks * allocationSize;
        return size < bytes ? size + allocationSize : size;
    }

    /**
     * Is there enough room allocated in the buffer to add the number of bytes needed
     * @param bytesNeeded the number of bytes needed
     * @return whether there is enough room
     */
    public boolean hasEnoughRoomFor(int bytesNeeded) {
        return (buffer.length - position) >= bytesNeeded;
    }

    /**
     * Clear the buffer, resetting its length
     *
     * @return this (fluent)
     */
    public ByteArrayPrimitiveBuilder clear() {
        position = 0;
        return this;
    }

    /**
     * Change the allocation size
     * @param allocationSize the new allocation size
     * @return this (fluent)
     */
    public ByteArrayPrimitiveBuilder setAllocationSize(int allocationSize) {
        int dftlAllo = defaultCharset == US_ASCII ? DEFAULT_ASCII_ALLOCATION : DEFAULT_OTHER_ALLOCATION;
        if (allocationSize < dftlAllo) {
            this.allocationSize = dftlAllo;
        }
        else {
            this.allocationSize = BuilderUtils.bufferAllocSize(allocationSize, dftlAllo);
        }
        return this;
    }

    /**
     * Get the current allocation size
     * @return the allocation size
     */
    public int getAllocationSize() {
        return allocationSize;
    }

    /**
     * Append a String representation of the number.
     * @param  i the number
     * @return this (fluent)
     */
    public ByteArrayPrimitiveBuilder append(int i) {
        append(Integer.toString(i).getBytes(US_ASCII)); // a number is always ascii
        return this;
    }

    /**
     * Append a String with the default charset.
     * If the src is null, the word 'null' is appended.
     * @param src The String from which bytes are to be read
     * @return this (fluent)
     */
    public ByteArrayPrimitiveBuilder append(String src) {
        return append(src, defaultCharset);
    }

    /**
     * Append a String with specified charset.
     * If the src is null, the word 'null' is appended.
     * @param src The String from which bytes are to be read
     * @param charset the charset for encoding
     * @return this (fluent)
     */
    public ByteArrayPrimitiveBuilder append(String src, Charset charset) {
        return src == null ? append(NULL, 0, 4) : append(src.getBytes(charset));
    }

    /**
     * Append a CharBuffer with default charset.
     * If the src is null, the word 'null' is appended.
     * @param src The CharBuffer from which bytes are to be read
     * @return this (fluent)
     */
    public ByteArrayPrimitiveBuilder append(CharBuffer src) {
        return append(src, defaultCharset);
    }

    /**
     * Append a CharBuffer with specified charset.
     * If the src is null, the word 'null' is appended.
     * @param src The CharBuffer from which bytes are to be read
     * @param charset the charset for encoding
     * @return this (fluent)
     */
    public ByteArrayPrimitiveBuilder append(CharBuffer src, Charset charset) {
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
    public ByteArrayPrimitiveBuilder append(byte b) {
        ensureCapacity(1);
        buffer[position++] = b;
        return this;
    }

    /**
     * Append a entire byte array
     * @param src The array from which bytes are to be read
     * @return this (fluent)
     */
    public ByteArrayPrimitiveBuilder append(byte[] src) {
        return append(src, 0, src.length);
    }

    /**
     * Append len bytes from the start of byte array
     * @param src The array from which bytes are to be read
     * @param len The number of bytes to be read from the given array
     * @return this (fluent)
     */
    public ByteArrayPrimitiveBuilder append(byte[] src, int len) {
        return append(src, 0, len);
    }

    /**
     * Append a byte array
     * @param src The array from which bytes are to be read
     * @param  offset The offset within the array of the first byte to be read;
     * @param  len The number of bytes to be read from the given array;
     * @return this (fluent)
     */
    public ByteArrayPrimitiveBuilder append(byte[] src, int offset, int len) {
        if (len > 0) {
            ensureCapacity(len);
            System.arraycopy(src, offset, buffer, position, len);
            position += len;
        }
        return this;
    }

    /**
     * Appends the data bytes from an existing byte array builder
     * @param bab the existing builder
     * @return this (fluent)
     */
    public ByteArrayPrimitiveBuilder append(ByteArrayPrimitiveBuilder bab) {
        if (bab != null) {
            append(bab.buffer, bab.length());
        }
        return this;
    }

    @Override
    public String toString() {
        return new String(buffer, 0, position, defaultCharset);
    }
}
