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

package io.nats.client.support;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Note that a ByteBuffer is always in one of two modes:
 * 
 * <ul>
 *   <li>Get mode: Valid data is between position and limit. Some documentation may refer to
 *       this as "read" mode, since the buffer is ready for various <code>.get()</code> method calls,
 *       however this can be confusing, since you would NEVER call the
 *       {@link java.nio.channels.ReadableByteChannel#read(ByteBuffer)} method when operating in
 *       this mode. This may also be referred to as "flush" mode, but nothing in the Buffer
 *       documentation refers to the term "flush", and thus we use the term "get" mode here.
 *   <li>Put mode: Valid data is between 0 and position. Some documentation may refer to this as
 *       "write" mode, since the buffer is ready for varous <code>.put()</code> method calls,
 *       however this can be confusing since you would NEVER call the
 *       {@link java.nio.channels.WritableByteChannel#write(ByteBuffer)} method when operating in
 *       this mode. This may also be referred to as "fill" mode, but the term fill is only used
 *       in one place in the Buffer.clear() documentation.
 * </ul>
 * 
 * All documentation will be using the terms "get mode" or "put mode".
 */
public interface BufferUtils {
    static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
    static final char SUBSTITUTE_CHAR = 0x2423;

    /**
     * It is not to uncommon to buffer data into a temporary buffer
     * and then append this temporary buffer into a destination buffer.
     * This method makes this operation easy since your temporary buffer
     * is typically in "put" mode and your destination is always in "put"
     * mode, and you need to take into account that the temporary buffer
     * may contain more bytes than your destination buffer, but you don't
     * want a BufferOverflowException to occur, instead you just want to
     * fullfill as many bytes from your temporary buffer as is possible.
     * 
     * See <code>org.eclipse.jetty.util.BufferUtil.append(ByteBuffer, ByteBuffer)</code>
     * for a similar method.
     * 
     * @param src is a buffer in "put" mode which will be flip'ed
     *     and then "safely" put into dst followed by a compact call.
     * @param dst is a buffer in "put" mode which will be populated
     *     from src.
     * @param max is the max bytes to transfer.
     * @return min(src.position(), dst.position(), max)
     */
    static int append(ByteBuffer src, ByteBuffer dst, int max) {
        if (src.position() < max) {
            max = src.position();
        }
        if (dst.remaining() < max) {
            max = dst.remaining();
        }
        src.flip();
        try {
            ByteBuffer slice = src.slice();
            slice.limit(max);
            dst.put(slice);
        } finally {
            src.position(max);
            src.compact();
        }
        return max;
    }

    /**
     * Delegates to {@link #append(ByteBuffer,ByteBuffer,int)}, with
     * max set to Integer.MAX_VALUE.
     * 
     * @param src is a buffer in "put" mode which will be flip'ed
     *     and then "safely" put into dst followed by a compact call.
     * @param dst is a buffer in "put" mode which will be populated
     *     from src.
     * @return min(src.position(), dst.position())
     */
    static int append(ByteBuffer src, ByteBuffer dst) {
        return append(src, dst, Integer.MAX_VALUE);
    }

    /**
     * Throws BufferUnderflowException if there are insufficient capacity in
     * buffer to fillfill the request.
     * 
     * @param readBuffer is in "put" mode (0 - position are valid)
     * @param reader is a reader used to populate the buffer if insufficient remaining
     *    bytes exist in buffer. May be null if buffer should not be populated.
     * @throws BufferUnderflowException if the buffer has insufficient capacity
     *    to read a full line.
     * @throws IOException if reader.read() throws this exception.
     * @return a line without line terminators or null if end of channel.
     */
    static String readLine(ByteBuffer readBuffer, ReadableByteChannel reader) throws IOException {
        if (null == readBuffer) {
            throw new NullPointerException("Expected non-null readBuffer");
        }
        int end = 0;
        boolean foundCR = false;
        int newlineLength = 1;
      FIND_END:
        while (true) {
            if (end >= readBuffer.position()) {
                if (readBuffer.position() == readBuffer.limit()) {
                    // Insufficient capacity in ByteBuffer to read a full line!
                    throw new BufferUnderflowException();
                }
                if (null == reader || reader.read(readBuffer) < 0) {
                    if (end > 0) {
                        if (!foundCR) {
                            newlineLength = 0;
                        }
                        break FIND_END;
                    }
                    return null;
                }
            }
            switch (readBuffer.get(end++)) {
            case '\r':
                if (foundCR) {
                    --end;
                    break FIND_END; // Legacy MAC end of line
                }
                foundCR = true;
                break;
            case '\n':
                if (foundCR) {
                    newlineLength++;
                }
                break FIND_END;
            default:
                if (foundCR) {
                    --end;
                    break FIND_END; // Legacy MAC end of line
                }
            }
        }

        String result;
        readBuffer.flip();
        try {
            ByteBuffer slice = readBuffer.slice();
            slice.limit(end - newlineLength);
            result = UTF_8.decode(slice).toString();
        } finally {
            readBuffer.position(end);
            readBuffer.compact();
        }
        return result;
    }

    static long remaining(ByteBuffer[] buffers, int offset, int length) {
        int total = 0;
        int end = offset + length;
        while (offset < end) {
            total += buffers[offset++].remaining();
        }
        return total;
    }

    /**
     * Utility method for stringifying a bytebuffer (use ByteBuffer.wrap(byte[])
     * if you want to stringify a byte array). Mostly useful for debugging or
     * tracing.
     * 
     * @param bytes is the byte buffer.
     * @param off is the offset within the byte buffer to begin.
     * @param len is the number of bytes to print.
     * @return a "hexdump" of the bytes
     */
    static String hexdump(ByteBuffer bytes, int off, int len) {
        int end = off + len;
        StringBuilder sb = new StringBuilder();
        for (int i=off; i < end;) {
            sb.append(String.format("%04x ", i));
            int start = i;
            do {
                int ch = bytes.get(i) & 0xFF;
                sb.append(" ");
                if (i % 16 == 8) {
                    sb.append(" ");
                }
                sb.append(HEX_ARRAY[ch >>> 4]);
                sb.append(HEX_ARRAY[ch & 0x0F]);
            } while (++i % 16 != 0 && i < end);
            if (i % 16 != 0) {
                sb.append(new String(new char[16 - i % 16]).replace("\0", "   "));
                if (i % 16 < 7) {
                    sb.append(" ");
                }
            }
            sb.append("  ");
            i = start;
            do {
                char ch = (char)bytes.get(i);
                if (ch < 0x21) {
                    // Control chars:
                    switch (ch) {
                    case ' ':
                        sb.append((char)0x2420);
                        break;
                    case '\t':
                        sb.append((char)0x2409);
                        break;
                    case '\r':
                        sb.append((char)0x240D);
                        break;
                    case '\n':
                        sb.append((char)0x2424);
                        break;
                    default:
                        sb.append(SUBSTITUTE_CHAR);
                    }
                } else if (ch < 0x7F) {
                    sb.append(ch);
                } else {
                    // control chars:
                    sb.append(SUBSTITUTE_CHAR);
                }
            } while (++i % 16 != 0 && i < end);
            sb.append("\n");
        }
        return sb.toString();
    }
}
