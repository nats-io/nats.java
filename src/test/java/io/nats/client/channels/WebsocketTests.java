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

package io.nats.client.channels;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import io.nats.client.support.WebsocketFrameHeader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static io.nats.client.support.WebsocketFrameHeader.OpCode;

public class WebsocketTests {
    private int[] testSizes = new int[] {
        1,
        2,
        3,
        4,
        8,
        16,
        32,
        64,
        128,
        256,
        512,
        1024,
        1432,
        1433,
        2048,
        4096,
        8192,
        16384,
        32768,
        65536,
        131072,
        262144
    };

    @Test
    public void testReadableByteChannelMaskedBinaryFin() throws IOException {
        for (int i=0; i < testSizes.length; i++) {
            byte[] fuzz = getFuzz(testSizes[i]);
            int maskingKey = new SecureRandom().nextInt();
            WebsocketReadableByteChannel win = websocketReadableByteChannel(
                new WebsocketFrameHeader()
                .withMask(maskingKey)
                .withOp(OpCode.BINARY, true),
                fuzz);
            byte[] got = new byte[testSizes[i]];
            assertEquals(got.length, win.read(ByteBuffer.wrap(got)));

            assertArrayEquals(fuzz, got);
            win.close();
        }
    }

    @Test
    public void testReadableByteChannelUnMaskedBinaryFin() throws IOException {
        for (int i=0; i < testSizes.length; i++) {
            byte[] fuzz = getFuzz(testSizes[i]);
            WebsocketReadableByteChannel win = websocketReadableByteChannel(
                new WebsocketFrameHeader()
                .withNoMask()
                .withOp(OpCode.BINARY, true),
                fuzz);
            byte[] got = new byte[testSizes[i]];
            assertEquals(got.length, win.read(ByteBuffer.wrap(got)));
            assertArrayEquals(fuzz, got);
            win.close();
        }
    }

    @Test
    public void testWritableByteChannelMaskedBinaryFin() throws IOException {
        for (int i=0; i < testSizes.length; i++) {
            byte[] fuzz = getFuzz(testSizes[i]);
            ByteBufferChannel out = new ByteBufferChannel(ByteBuffer.allocate(
                WebsocketFrameHeader.MAX_FRAME_HEADER_SIZE + fuzz.length));
            WebsocketWritableByteChannel wout = new WebsocketWritableByteChannel(
                out, true, OpCode.BINARY, new SecureRandom());

            // NOTE: this API will modify fuzz, so we need to take a copy:
            wout.write(ByteBuffer.wrap(Arrays.copyOf(fuzz, fuzz.length)));

            ByteBuffer got = out.getByteBuffer();
            got.flip();

            // Validate the header:
            WebsocketFrameHeader header = new WebsocketFrameHeader();
            int headerLength = header.deserialize(got);
            assertEquals(headerLength, got.position());
            assertEquals(OpCode.BINARY, header.getOpCode());
            assertEquals(fuzz.length, header.getPayloadLength());
            assertTrue(header.isMasked());
 
            // Must be masked!
            assertNotEquals(ByteBuffer.wrap(fuzz), got);

            // So, unmask it:
            ByteBuffer slice = got.slice();
            slice.position(slice.position() + header.getIntPayloadLength());
            header.filterPayload(slice, header.getIntPayloadLength());
            assertEquals(ByteBuffer.wrap(fuzz), got);

            wout.close();
        }
    }

    // Simply Copy & Paste, to introspect raw bytes easily.
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
    private static final char SUBSTITUTE_CHAR = 0x2423;
    private static String hexdump(ByteBuffer bytes, int off, int len) {
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

    @Test
    public void testWritableByteChannelUnMaskedBinaryFin() throws IOException {
        for (int i=0; i < testSizes.length; i++) {
            byte[] fuzz = getFuzz(testSizes[i]);
            ByteBufferChannel out = new ByteBufferChannel(ByteBuffer.allocate(1024));
            WebsocketWritableByteChannel wout = new WebsocketWritableByteChannel(
                out, false, OpCode.BINARY, new SecureRandom());

            wout.write(ByteBuffer.wrap(fuzz));

            ByteBuffer got = out.getByteBuffer();
            got.flip();

            // Validate the header:
            WebsocketFrameHeader header = new WebsocketFrameHeader();
            int headerLength = header.deserialize(got);
            assertEquals(headerLength, got.position());
            assertEquals(OpCode.BINARY, header.getOpCode());
            assertEquals(fuzz.length, header.getPayloadLength());
            assertFalse(header.isMasked());

            // Must NOT be masked!
            assertEquals(ByteBuffer.wrap(fuzz), got);

            wout.close();
        }
    }

    @Test
    public void testClose() throws IOException {
        ByteBufferChannel out = new ByteBufferChannel(ByteBuffer.allocate(1024));
        WebsocketWritableByteChannel wout = new WebsocketWritableByteChannel(
            out, false, OpCode.BINARY, new SecureRandom());

        wout.close();

        // Shouldn't have written anything:
        assertEquals(0, out.getByteBuffer().position());
    }

    @Test
    public void testFrameHeaderCoverage() throws IOException {
        assertEquals(OpCode.CONTINUATION, OpCode.of(0));
        assertEquals(OpCode.TEXT, OpCode.of(1));
        assertEquals(OpCode.BINARY, OpCode.of(2));
        assertEquals(OpCode.CLOSE, OpCode.of(8));
        assertEquals(OpCode.PING, OpCode.of(9));
        assertEquals(OpCode.PONG, OpCode.of(10));
        assertEquals(OpCode.UNKNOWN, OpCode.of(11));

        WebsocketFrameHeader header = new WebsocketFrameHeader();
        assertEquals(0xFF, header.withMask(0xFF).getMaskingKey());
        assertEquals(0, header.deserialize(ByteBuffer.allocate(1)));
        assertEquals(0, header.deserialize(ByteBuffer.wrap(new byte[]{2, 127})));
    }

    private byte[] getFuzz(int size) {
        byte[] fuzz = new byte[size];
        new SecureRandom().nextBytes(fuzz);
        return fuzz;
    }

    /**
     * Create a ByteArrayReadableByteChannel with a websocket frame header and payload.
     * 
     */
    private WebsocketReadableByteChannel websocketReadableByteChannel(WebsocketFrameHeader header, byte[] payload) {
        ByteBuffer buffer = ByteBuffer.allocate(payload.length + WebsocketFrameHeader.MAX_FRAME_HEADER_SIZE);

        // Set payload length:
        header.withPayloadLength(payload.length);

        // write the header into a buffer:
        header.serialize(buffer);

        // write the payload:
        buffer.put(payload);

        // filter the payload:
        header.filterPayload(buffer, payload.length);

        return new WebsocketReadableByteChannel(
            new ByteBufferChannel(buffer),
            ByteBuffer.allocate(WebsocketFrameHeader.MAX_FRAME_HEADER_SIZE));
    }
}
