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

package io.nats.client.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Arrays;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static io.nats.client.impl.WebsocketFrameHeader.OpCode;

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
        1432, // WebsocketOutputStream buffer threshold, will fragment after this.
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
    public void testInputStreamMaskedBinaryFin() throws IOException {
        for (int i=0; i < testSizes.length; i++) {
            byte[] fuzz = getFuzz(testSizes[i]);
            int maskingKey = new SecureRandom().nextInt();
            InputStream in = websocketInputStream(
                new WebsocketFrameHeader()
                .withMask(maskingKey)
                .withOp(OpCode.BINARY, true),
                fuzz);
            WebsocketInputStream win = new WebsocketInputStream(in);
            byte[] got = new byte[testSizes[i]];
            if (fuzz.length == 1) {
                got[0] = (byte)(win.read() & 0xFF);
            } else {
                assertEquals(got.length, win.read(got));
            }
            assertArrayEquals(fuzz, got);
            win.close();
        }
    }

    @Test
    public void testInputStreamUnMaskedBinaryFin() throws IOException {
        for (int i=0; i < testSizes.length; i++) {
            byte[] fuzz = getFuzz(testSizes[i]);
            InputStream in = websocketInputStream(
                new WebsocketFrameHeader()
                .withNoMask()
                .withOp(OpCode.BINARY, true),
                fuzz);
            WebsocketInputStream win = new WebsocketInputStream(in);
            byte[] got = new byte[testSizes[i]];
            assertEquals(got.length, win.read(got));
            assertArrayEquals(fuzz, got);
            win.close();
        }
    }

    @Test
    public void testOutputStreamMaskedBinaryFin() throws IOException {
        for (int i=0; i < testSizes.length; i++) {
            byte[] fuzz = getFuzz(testSizes[i]);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            WebsocketOutputStream wout = new WebsocketOutputStream(out, true);
            if (1 == fuzz.length) {
                wout.write(fuzz[0]);
            } else {
                // NOTE: this API will modify fuzz, so we need to take a copy:
                wout.write(Arrays.copyOf(fuzz, fuzz.length));
            }
            byte[] got = out.toByteArray();

            // Validate the header:
            WebsocketFrameHeader header = new WebsocketFrameHeader();
            int headerLength = header.write(got, 0, got.length);
            assertEquals(OpCode.BINARY, header.getOpCode());
            assertEquals(fuzz.length, header.getPayloadLength());
            assertTrue(header.isMasked());
 
            // Validate the payload:
            byte[] gotPayload = Arrays.copyOfRange(got, headerLength, got.length);
            // Must be masked!
            assertFalse(Arrays.equals(gotPayload, fuzz));
            assertEquals(gotPayload.length, header.filterPayload(gotPayload, 0, gotPayload.length));
            assertArrayEquals(gotPayload, fuzz, "size=" + fuzz.length);

            wout.close();
        }
    }

    @Test
    public void testOutputStreamUnMaskedBinaryFin() throws IOException {
        for (int i=0; i < testSizes.length; i++) {
            byte[] fuzz = getFuzz(testSizes[i]);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            WebsocketOutputStream wout = new WebsocketOutputStream(out, false);
            wout.write(fuzz);
            byte[] got = out.toByteArray();

            // Validate the header:
            WebsocketFrameHeader header = new WebsocketFrameHeader();
            int headerLength = header.write(got, 0, got.length);
            assertEquals(OpCode.BINARY, header.getOpCode());
            assertEquals(fuzz.length, header.getPayloadLength());
            assertFalse(header.isMasked());

            // Validate the payload:
            byte[] gotPayload = Arrays.copyOfRange(got, headerLength, got.length);

            // Must NOT be masked!
            assertArrayEquals(gotPayload, fuzz);

            wout.close();
        }
    }

    @Test
    public void testClose() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        WebsocketOutputStream wout = new WebsocketOutputStream(out, false);
        wout.close();
        byte[] got = out.toByteArray();

        // Validate the header:
        WebsocketFrameHeader header = new WebsocketFrameHeader();
        assertEquals(2, header.write(got, 0, got.length));
        assertEquals(OpCode.CLOSE, header.getOpCode());
        assertEquals(0, header.getPayloadLength());
        assertFalse(header.isMasked());
        assertTrue(header.isFinal());
        assertTrue(header.isPayloadEmpty());
    }

    @Test
    public void testInputCoverage() throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(new byte[0]);
        WebsocketInputStream win = new WebsocketInputStream(in);
        assertFalse(win.markSupported());
        win.mark(0);
        assertEquals(0, win.available());
        win.close();
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
        assertEquals(0, header.write(new byte[1], 0, 1));
        assertEquals(0, header.write(new byte[]{2, 127}, 0, 2));
    }

    private byte[] getFuzz(int size) {
        byte[] fuzz = new byte[size];
        new SecureRandom().nextBytes(fuzz);
        return fuzz;
    }

    /**
     * Create a ByteArrayInputStream with a websocket frame header and payload.
     * 
     */
    private InputStream websocketInputStream(WebsocketFrameHeader header, byte[] payload) {
        ByteBuffer buffer = ByteBuffer.allocate(payload.length + WebsocketFrameHeader.MAX_FRAME_HEADER_SIZE);

        // Set payload length:
        header.withPayloadLength(payload.length);

        // write the header into a buffer:
        buffer.position(
            buffer.position() +
            header.read(buffer.array(), buffer.position(), buffer.limit()));

        // write the payload:
        int position = buffer.position();
        buffer.put(payload);

        // filter the payload:
        int filterLength = header.filterPayload(buffer.array(), position, buffer.position() - position);

        assertEquals(buffer.position() - position, filterLength);
        return new ByteArrayInputStream(buffer.array(), 0, buffer.position());
    }    
}
