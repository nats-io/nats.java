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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.Test;

import io.nats.client.NatsTestServer;
import io.nats.client.Options;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static io.nats.client.support.WebsocketFrameHeader.OpCode;

public class WebsocketTests {
    private int[] testSizes = new int[] { 1, 2, 3, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 1432, // WebsocketOutputStream
                                                                                                // buffer threshold,
                                                                                                // will fragment after
                                                                                                // this.
            1433, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144 };

    @Test
    public void testInputStreamMaskedBinaryFin() throws IOException {
        for (int i = 0; i < testSizes.length; i++) {
            byte[] fuzz = getFuzz(testSizes[i]);
            int maskingKey = new SecureRandom().nextInt();
            InputStream in = websocketInputStream(
                    new WebsocketFrameHeader().withMask(maskingKey).withOp(OpCode.BINARY, true), fuzz);
            WebsocketInputStream win = new WebsocketInputStream(in);
            byte[] got = new byte[testSizes[i]];
            if (fuzz.length == 1) {
                got[0] = (byte) (win.read() & 0xFF);
            } else {
                assertEquals(got.length, win.read(got));
            }
            assertArrayEquals(fuzz, got);
            win.close();
        }
    }

    @Test
    public void testInputStreamUnMaskedBinaryFin() throws IOException {
        for (int i = 0; i < testSizes.length; i++) {
            byte[] fuzz = getFuzz(testSizes[i]);
            InputStream in = websocketInputStream(new WebsocketFrameHeader().withNoMask().withOp(OpCode.BINARY, true),
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
        for (int i = 0; i < testSizes.length; i++) {
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
        for (int i = 0; i < testSizes.length; i++) {
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
        assertEquals(0, header.write(new byte[] { 2, 127 }, 0, 2));
    }

    private byte[] getFuzz(int size) {
        byte[] fuzz = new byte[size];
        new SecureRandom().nextBytes(fuzz);
        return fuzz;
    }

    class DelegatedException extends RuntimeException {
    }

    @Test
    public void testWebSocketCoverage() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/ws.conf", false)) {
            try (Socket tcpSocket = new Socket("localhost", ts.getPort())) {
                WebSocket webSocket = new WebSocket(new Socket() {
                    @Override
                    public InputStream getInputStream() throws IOException {
                        return tcpSocket.getInputStream();
                    }

                    @Override
                    public OutputStream getOutputStream() throws IOException {
                        return tcpSocket.getOutputStream();
                    }

                    @Override
                    public InetAddress getInetAddress() {
                        throw new DelegatedException();
                    }

                    @Override
                    public InetAddress getLocalAddress() {
                        throw new DelegatedException();
                    }

                    @Override
                    public int getPort() {
                        throw new DelegatedException();
                    }

                    @Override
                    public int getLocalPort() {
                        throw new DelegatedException();
                    }

                    @Override
                    public SocketAddress getRemoteSocketAddress() {
                        throw new DelegatedException();
                    }

                    @Override
                    public SocketAddress getLocalSocketAddress() {
                        throw new DelegatedException();
                    }

                    @Override
                    public void setTcpNoDelay(boolean on) throws SocketException {
                        throw new DelegatedException();
                    }

                    @Override
                    public boolean getTcpNoDelay() throws SocketException {
                        throw new DelegatedException();
                    }

                    @Override
                    public void setSoLinger(boolean on, int linger) throws SocketException {
                        throw new DelegatedException();
                    }

                    @Override
                    public int getSoLinger() throws SocketException {
                        throw new DelegatedException();
                    }

                    @Override
                    public void sendUrgentData(int data) throws IOException {
                        throw new DelegatedException();
                    }

                    @Override
                    public void setOOBInline(boolean on) throws SocketException {
                        throw new DelegatedException();
                    }

                    @Override
                    public boolean getOOBInline() throws SocketException {
                        throw new DelegatedException();
                    }

                    @Override
                    public void setSoTimeout(int timeout) throws SocketException {
                        throw new DelegatedException();
                    }

                    @Override
                    public int getSoTimeout() throws SocketException {
                        throw new DelegatedException();
                    }

                    @Override
                    public void setSendBufferSize(int size) throws SocketException {
                        throw new DelegatedException();
                    }

                    @Override
                    public int getSendBufferSize() throws SocketException {
                        throw new DelegatedException();
                    }

                    @Override
                    public void setReceiveBufferSize(int size) throws SocketException {
                        throw new DelegatedException();
                    }

                    @Override
                    public int getReceiveBufferSize() throws SocketException {
                        throw new DelegatedException();
                    }

                    @Override
                    public void setKeepAlive(boolean on) throws SocketException {
                        throw new DelegatedException();
                    }

                    @Override
                    public boolean getKeepAlive() throws SocketException {
                        throw new DelegatedException();
                    }

                    @Override
                    public void setTrafficClass(int tc) throws SocketException {
                        throw new DelegatedException();
                    }

                    @Override
                    public int getTrafficClass() throws SocketException {
                        throw new DelegatedException();
                    }

                    @Override
                    public void setReuseAddress(boolean on) throws SocketException {
                        throw new DelegatedException();
                    }

                    @Override
                    public boolean getReuseAddress() throws SocketException {
                        throw new DelegatedException();
                    }

                    @Override
                    public synchronized void close() throws IOException {
                        tcpSocket.close();
                    }

                    @Override
                    public void shutdownInput() throws IOException {
                        throw new DelegatedException();
                    }

                    @Override
                    public void shutdownOutput() throws IOException {
                        throw new DelegatedException();
                    }

                    @Override
                    public boolean isConnected() {
                        throw new DelegatedException();
                    }

                    @Override
                    public boolean isBound() {
                        throw new DelegatedException();
                    }

                    @Override
                    public boolean isClosed() {
                        throw new DelegatedException();
                    }

                    @Override
                    public boolean isInputShutdown() {
                        throw new DelegatedException();
                    }

                    @Override
                    public boolean isOutputShutdown() {
                        throw new DelegatedException();
                    }

                    @Override
                    public void setPerformancePreferences(int connectionTime, int latency, int bandwidth) {
                        throw new DelegatedException();
                    }
                }, "host", Collections.emptyList());

                // Unsupported methods:
                assertThrows(UnsupportedOperationException.class, () -> webSocket.connect(null));
                assertThrows(UnsupportedOperationException.class, () -> webSocket.connect(null, 0));
                assertThrows(UnsupportedOperationException.class, () -> webSocket.bind(null));
                assertThrows(UnsupportedOperationException.class, () -> webSocket.getChannel());

                // Delegated methods:
                assertThrows(DelegatedException.class, () -> webSocket.getInetAddress());
                assertThrows(DelegatedException.class, () -> webSocket.getLocalAddress());
                assertThrows(DelegatedException.class, () -> webSocket.getPort());
                assertThrows(DelegatedException.class, () -> webSocket.getLocalPort());
                assertThrows(DelegatedException.class, () -> webSocket.getRemoteSocketAddress());
                assertThrows(DelegatedException.class, () -> webSocket.getLocalSocketAddress());
                assertThrows(DelegatedException.class, () -> webSocket.setTcpNoDelay(true));
                assertThrows(DelegatedException.class, () -> webSocket.getTcpNoDelay());
                assertThrows(DelegatedException.class, () -> webSocket.setSoLinger(true, 0));
                assertThrows(DelegatedException.class, () -> webSocket.getSoLinger());
                assertThrows(DelegatedException.class, () -> webSocket.sendUrgentData(1));
                assertThrows(DelegatedException.class, () -> webSocket.setOOBInline(true));
                assertThrows(DelegatedException.class, () -> webSocket.getOOBInline());
                assertThrows(DelegatedException.class, () -> webSocket.setSoTimeout(1));
                assertThrows(DelegatedException.class, () -> webSocket.getSoTimeout());
                assertThrows(DelegatedException.class, () -> webSocket.setSendBufferSize(1));
                assertThrows(DelegatedException.class, () -> webSocket.getSendBufferSize());
                assertThrows(DelegatedException.class, () -> webSocket.setReceiveBufferSize(1));
                assertThrows(DelegatedException.class, () -> webSocket.getReceiveBufferSize());
                assertThrows(DelegatedException.class, () -> webSocket.setKeepAlive(true));
                assertThrows(DelegatedException.class, () -> webSocket.getKeepAlive());
                assertThrows(DelegatedException.class, () -> webSocket.setTrafficClass(1));
                assertThrows(DelegatedException.class, () -> webSocket.getTrafficClass());
                assertThrows(DelegatedException.class, () -> webSocket.setReuseAddress(true));
                assertThrows(DelegatedException.class, () -> webSocket.getReuseAddress());
                assertThrows(DelegatedException.class, () -> webSocket.shutdownInput());
                assertThrows(DelegatedException.class, () -> webSocket.shutdownOutput());
                assertThrows(DelegatedException.class, () -> webSocket.isConnected());
                assertThrows(DelegatedException.class, () -> webSocket.isBound());
                assertThrows(DelegatedException.class, () -> webSocket.isClosed());
                assertThrows(DelegatedException.class, () -> webSocket.isInputShutdown());
                assertThrows(DelegatedException.class, () -> webSocket.isOutputShutdown());
                assertThrows(DelegatedException.class, () -> webSocket.setPerformancePreferences(1, 1, 1));
            }
        }
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
        buffer.position(buffer.position() + header.read(buffer.array(), buffer.position(), buffer.limit()));

        // write the payload:
        int position = buffer.position();
        buffer.put(payload);

        // filter the payload:
        int filterLength = header.filterPayload(buffer.array(), position, buffer.position() - position);

        assertEquals(buffer.position() - position, filterLength);
        return new ByteArrayInputStream(buffer.array(), 0, buffer.position());
    }
}
