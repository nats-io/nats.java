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
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.nats.client.NatsTestServer;
import io.nats.client.Options;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static io.nats.client.support.WebsocketFrameHeader.OpCode;
import static java.nio.charset.StandardCharsets.UTF_8;

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

    @Test
    public void testHandshakeFailures() throws Exception {
        byte[] buffer = new byte[1024];
        Arrays.fill(buffer, (byte) 'a');

        testWithWriter(out -> {
            for (int i = 0; i < 1000; i++) {
                out.write(buffer, 0, buffer.length);
            }
            out.close();
        }, "Expected HTTP response line not to exceed ");

        testWithWriter(out -> {
            OutputStreamWriter writer = new OutputStreamWriter(out, UTF_8);
            writer.append("HTTP/1.1 101 Switching Protocols\r\n");
            writer.flush();
            for (int i = 0; i < 1000; i++) {
                out.write(buffer, 0, buffer.length);
            }
            out.close();
        }, "Expected HTTP header to not exceed ");

        testWithWriter(out -> {
            OutputStreamWriter writer = new OutputStreamWriter(out, UTF_8);
            writer.append("HTTP/1.1 101 Switching Protocols\r\n");
            writer.append("missing-colon\r\n");
            writer.append("\r\n");
            writer.close();
        }, "Expected HTTP header to contain ':', but got missing-colon");

        testWithWriter(out -> {
            OutputStreamWriter writer = new OutputStreamWriter(out, UTF_8);
            writer.append("HTTP/1.1 101 Switching Protocols\r\n");
            for (int i = 0; i < 1000; i++) {
                writer.append("a-" + i + ": Test\r\n");
            }
            writer.close();
        }, "Exceeded max HTTP headers");

        testWithWriter(out -> {
            OutputStreamWriter writer = new OutputStreamWriter(out, UTF_8);
            writer.append("HTTP/1.1 101 Switching Protocols\r\n");
            writer.append("Connection: Upgrade\r\n");
            writer.append("Sec-Websocket-Accept: DEADBEEF\r\n");
            writer.append("\r\n");
            writer.close();
        }, "Expected HTTP `Upgrade: websocket` header");

        testWithWriter(out -> {
            OutputStreamWriter writer = new OutputStreamWriter(out, UTF_8);
            writer.append("HTTP/1.1 101 Switching Protocols\n");
            writer.append("Upgrade: Websocket\n");
            writer.append("Sec-Websocket-Accept: DEADBEEF\n");
            writer.append("\n");
            writer.close();
        }, "Expected HTTP `Connection: Upgrade` header");

        testWithWriter(out -> {
            OutputStreamWriter writer = new OutputStreamWriter(out, UTF_8);
            writer.append("HTTP/1.1 101 Switching Protocols\r\n");
            writer.append("Upgrade: Websocket\r\n");
            writer.append("Connection: Upgrade\r\n");
            writer.append("Sec-Websocket-Accept: DEADBEEF\r\n");
            writer.append("\r\n");
            writer.close();
        }, "Expected HTTP `Sec-WebSocket-Accept: ");

        // Premature EOF:
        testWithWriter(out -> {
            OutputStreamWriter writer = new OutputStreamWriter(out, UTF_8);
            writer.append("HTTP/1.1 101 Switching Protocols\r\n");
            writer.append("Upgrade: Websocket\r\n");
            writer.append("Connection: Upgrade\r\n");
            writer.append("Sec-Websocket-Accept: DEADBEEF");
            writer.flush();
        }, "Expected HTTP `Sec-WebSocket-Accept: ");
    }

    @FunctionalInterface
    interface OutputStreamWrite {
        public void write(OutputStream out) throws IOException;
    }

    private void testWithWriter(OutputStreamWrite writer, String msgStartsWith) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try (ServerSocket serverSocket = new ServerSocket(0, 10, InetAddress.getByName("localhost"))) {
            Future<?> serverFuture = executor.submit(() -> {
                Socket client = serverSocket.accept();
                InputStream in = client.getInputStream();
                Future<?> readFuture = executor.submit(() -> {
                    byte[] buffer = new byte[1024];
                    try {
                        while (in.read(buffer) >= 0) {
                        }
                    } catch (SocketException ex) {
                        // Expect this failure:
                        if (!"Connection reset".equals(ex.getMessage()) && !"Socket closed".equals(ex.getMessage())) {
                            throw ex;
                        }
                    }
                    return null;
                });
                try {
                    writer.write(client.getOutputStream());
                    if (!client.isClosed()) {
                        client.shutdownOutput();
                    }
                } catch (SocketException ex) {
                    // Expect this failure:
                    if (!"Broken pipe (Write failed)".equals(ex.getMessage())) {
                        throw ex;
                    }
                }
                readFuture.get();
                return null;
            });
            Socket client = new Socket("localhost", serverSocket.getLocalPort());
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> new WebSocket(client, "localhost", Collections.emptyList()));
            assertTrue(ex.getMessage().startsWith(msgStartsWith), ex.getMessage());
            client.close();
            serverFuture.get();
        }
        executor.shutdownNow();
    }

    @Test
    public void testWebSocketCoverage() throws Exception {
        AtomicReference<String> lastMethod = new AtomicReference<>();
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
                    public synchronized void close() throws IOException {
                        tcpSocket.close();
                    }

                    @Override
                    public InetAddress getInetAddress() {
                        lastMethod.set("getInetAddress");
                        return null;
                    }

                    @Override
                    public InetAddress getLocalAddress() {
                        lastMethod.set("getLocalAddress");
                        return null;
                    }

                    @Override
                    public int getPort() {
                        lastMethod.set("getPort");
                        return 0;
                    }

                    @Override
                    public int getLocalPort() {
                        lastMethod.set("getLocalPort");
                        return 0;
                    }

                    @Override
                    public SocketAddress getRemoteSocketAddress() {
                        lastMethod.set("getRemoteSocketAddress");
                        return null;
                    }

                    @Override
                    public SocketAddress getLocalSocketAddress() {
                        lastMethod.set("getLocalSocketAddress");
                        return null;
                    }

                    @Override
                    public void setTcpNoDelay(boolean on) throws SocketException {
                        lastMethod.set("setTcpNoDelay");
                    }

                    @Override
                    public boolean getTcpNoDelay() throws SocketException {
                        lastMethod.set("getTcpNoDelay");
                        return true;
                    }

                    @Override
                    public void setSoLinger(boolean on, int linger) throws SocketException {
                        lastMethod.set("setSoLinger");
                    }

                    @Override
                    public int getSoLinger() throws SocketException {
                        lastMethod.set("getSoLinger");
                        return 0;
                    }

                    @Override
                    public void sendUrgentData(int data) throws IOException {
                        lastMethod.set("sendUrgentData");
                    }

                    @Override
                    public void setOOBInline(boolean on) throws SocketException {
                        lastMethod.set("setOOBInline");
                    }

                    @Override
                    public boolean getOOBInline() throws SocketException {
                        lastMethod.set("getOOBInline");
                        return false;
                    }

                    @Override
                    public void setSoTimeout(int timeout) throws SocketException {
                        lastMethod.set("setSoTimeout");
                    }

                    @Override
                    public int getSoTimeout() throws SocketException {
                        lastMethod.set("getSoTimeout");
                        return 1000;
                    }

                    @Override
                    public void setSendBufferSize(int size) throws SocketException {
                        lastMethod.set("setSendBufferSize");
                    }

                    @Override
                    public int getSendBufferSize() throws SocketException {
                        lastMethod.set("getSendBufferSize");
                        return 1024;
                    }

                    @Override
                    public void setReceiveBufferSize(int size) throws SocketException {
                        lastMethod.set("setReceiveBufferSize");
                    }

                    @Override
                    public int getReceiveBufferSize() throws SocketException {
                        lastMethod.set("getReceiveBufferSize");
                        return 1024;
                    }

                    @Override
                    public void setKeepAlive(boolean on) throws SocketException {
                        lastMethod.set("setKeepAlive");
                    }

                    @Override
                    public boolean getKeepAlive() throws SocketException {
                        lastMethod.set("getKeepAlive");
                        return false;
                    }

                    @Override
                    public void setTrafficClass(int tc) throws SocketException {
                        lastMethod.set("setTrafficClass");
                    }

                    @Override
                    public int getTrafficClass() throws SocketException {
                        lastMethod.set("getTrafficClass");
                        return 0;
                    }

                    @Override
                    public void setReuseAddress(boolean on) throws SocketException {
                        lastMethod.set("setReuseAddress");
                    }

                    @Override
                    public boolean getReuseAddress() throws SocketException {
                        lastMethod.set("getReuseAddress");
                        return false;
                    }

                    @Override
                    public void shutdownInput() throws IOException {
                        lastMethod.set("shutdownInput");
                    }

                    @Override
                    public void shutdownOutput() throws IOException {
                        lastMethod.set("shutdownOutput");
                    }

                    @Override
                    public boolean isConnected() {
                        lastMethod.set("isConnected");
                        return true;
                    }

                    @Override
                    public boolean isBound() {
                        lastMethod.set("isBound");
                        return true;
                    }

                    @Override
                    public boolean isClosed() {
                        lastMethod.set("isClosed");
                        return false;
                    }

                    @Override
                    public boolean isInputShutdown() {
                        lastMethod.set("isInputShutdown");
                        return false;
                    }

                    @Override
                    public boolean isOutputShutdown() {
                        lastMethod.set("isOutputShutdown");
                        return false;
                    }

                    @Override
                    public void setPerformancePreferences(int connectionTime, int latency, int bandwidth) {
                        lastMethod.set("setPerformancePreferences");
                    }
                }, "host", Collections.emptyList());

                // Unsupported methods:
                assertThrows(UnsupportedOperationException.class, () -> webSocket.connect(null));
                assertThrows(UnsupportedOperationException.class, () -> webSocket.connect(null, 0));
                assertThrows(UnsupportedOperationException.class, () -> webSocket.bind(null));
                assertThrows(UnsupportedOperationException.class, () -> webSocket.getChannel());

                // Delegated methods:
                webSocket.getInetAddress();
                assertEquals("getInetAddress", lastMethod.get());

                webSocket.getLocalAddress();
                assertEquals("getLocalAddress", lastMethod.get());

                webSocket.getPort();
                assertEquals("getPort", lastMethod.get());

                webSocket.getLocalPort();
                assertEquals("getLocalPort", lastMethod.get());

                webSocket.getRemoteSocketAddress();
                assertEquals("getRemoteSocketAddress", lastMethod.get());

                webSocket.getLocalSocketAddress();
                assertEquals("getLocalSocketAddress", lastMethod.get());

                webSocket.setTcpNoDelay(true);
                assertEquals("setTcpNoDelay", lastMethod.get());

                webSocket.getTcpNoDelay();
                assertEquals("getTcpNoDelay", lastMethod.get());

                webSocket.setSoLinger(true, 0);
                assertEquals("setSoLinger", lastMethod.get());

                webSocket.getSoLinger();
                assertEquals("getSoLinger", lastMethod.get());

                webSocket.sendUrgentData(1);
                assertEquals("sendUrgentData", lastMethod.get());

                webSocket.setOOBInline(true);
                assertEquals("setOOBInline", lastMethod.get());

                webSocket.getOOBInline();
                assertEquals("getOOBInline", lastMethod.get());

                webSocket.setSoTimeout(1);
                assertEquals("setSoTimeout", lastMethod.get());

                webSocket.getSoTimeout();
                assertEquals("getSoTimeout", lastMethod.get());

                webSocket.setSendBufferSize(1);
                assertEquals("setSendBufferSize", lastMethod.get());

                webSocket.getSendBufferSize();
                assertEquals("getSendBufferSize", lastMethod.get());

                webSocket.setReceiveBufferSize(1);
                assertEquals("setReceiveBufferSize", lastMethod.get());

                webSocket.getReceiveBufferSize();
                assertEquals("getReceiveBufferSize", lastMethod.get());

                webSocket.setKeepAlive(true);
                assertEquals("setKeepAlive", lastMethod.get());

                webSocket.getKeepAlive();
                assertEquals("getKeepAlive", lastMethod.get());

                webSocket.setTrafficClass(1);
                assertEquals("setTrafficClass", lastMethod.get());

                webSocket.getTrafficClass();
                assertEquals("getTrafficClass", lastMethod.get());

                webSocket.setReuseAddress(true);
                assertEquals("setReuseAddress", lastMethod.get());

                webSocket.getReuseAddress();
                assertEquals("getReuseAddress", lastMethod.get());

                webSocket.shutdownInput();
                assertEquals("shutdownInput", lastMethod.get());

                webSocket.shutdownOutput();
                assertEquals("shutdownOutput", lastMethod.get());

                webSocket.isConnected();
                assertEquals("isConnected", lastMethod.get());

                webSocket.isBound();
                assertEquals("isBound", lastMethod.get());

                webSocket.isClosed();
                assertEquals("isClosed", lastMethod.get());

                webSocket.isInputShutdown();
                assertEquals("isInputShutdown", lastMethod.get());

                webSocket.isOutputShutdown();
                assertEquals("isOutputShutdown", lastMethod.get());

                webSocket.setPerformancePreferences(1, 1, 1);
                assertEquals("setPerformancePreferences", lastMethod.get());
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
