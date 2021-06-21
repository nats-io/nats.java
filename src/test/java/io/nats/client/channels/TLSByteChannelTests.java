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

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import org.junit.jupiter.api.Test;

import io.nats.client.NatsTestServer;
import io.nats.client.TestSSLUtils;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

public class TLSByteChannelTests {
    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

    @Test
    public void testShortAppRead() throws Exception {
        // Scenario: Net read TLS frame which is larger than the read buffer.
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tls.conf", false)) {
            URI uri = new URI(ts.getURI());
            NatsChannel socket = SocketNatsChannel.factory().connect(uri, Duration.ofSeconds(2));
            ByteBuffer info = ByteBuffer.allocate(1024 * 1024);
            socket.read(info);
    
            TLSByteChannel tls = new TLSByteChannel(socket, createSSLEngine(uri));

            write(tls, "CONNECT {}\r\n");

            ByteBuffer oneByte = ByteBuffer.allocate(1);
            assertEquals(1, tls.read(oneByte));
            assertEquals(1, oneByte.position());
            assertEquals((byte)'+', oneByte.get(0)); // got 0?
            oneByte.clear();

            assertEquals(1, tls.read(oneByte));
            assertEquals((byte)'O', oneByte.get(0));
            oneByte.clear();

            assertEquals(1, tls.read(oneByte));
            assertEquals((byte)'K', oneByte.get(0));
            oneByte.clear();

            // Follow up with a larger buffer read,
            // ...to ensure that we don't block on
            // a net read:
            info.clear();
            int result = tls.read(info);
            assertEquals(2, result);
            assertEquals(2, info.position());
            assertEquals((byte)'\r', info.get(0));
            assertEquals((byte)'\n', info.get(1));
            oneByte.clear();

            assertTrue(tls.isOpen());
            assertTrue(socket.isOpen());
            tls.close();
            assertFalse(tls.isOpen());
            assertFalse(socket.isOpen());
        }
    }

    @Test
    public void testImmediateClose() throws Exception {
        // Scenario: Net read TLS frame which is larger than the read buffer.
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tls.conf", false)) {
            URI uri = new URI(ts.getURI());
            NatsChannel socket = SocketNatsChannel.factory().connect(uri, Duration.ofSeconds(2));
            ByteBuffer info = ByteBuffer.allocate(1024 * 1024);
            socket.read(info);
    
            TLSByteChannel tls = new TLSByteChannel(socket, createSSLEngine(uri));

            assertTrue(tls.isOpen());
            assertTrue(socket.isOpen());
            tls.close();
            assertFalse(tls.isOpen());
            assertFalse(socket.isOpen());
        }
    }

    @Test
    public void testRenegotiation() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tls.conf", false)) {
            URI uri = new URI(ts.getURI());
            NatsChannel socket = SocketNatsChannel.factory().connect(uri, Duration.ofSeconds(2));
            ByteBuffer readBuffer = ByteBuffer.allocate(1024 * 1024);
            socket.read(readBuffer);
    
            SSLEngine sslEngine = createSSLEngine(uri);
            TLSByteChannel tls = new TLSByteChannel(socket, sslEngine);

            write(tls, "CONNECT {}\r\n");

            readBuffer.clear();
            tls.read(readBuffer);
            readBuffer.flip();
            assertEquals(ByteBuffer.wrap("+OK\r\n".getBytes(UTF_8)), readBuffer);

            // Now force a renegotiation:
            sslEngine.getSession().invalidate();
            sslEngine.beginHandshake();

            // nats-server doesn't support renegotion, we just get this error:
            // javax.net.ssl.SSLException: Received fatal alert: unexpected_message
            assertThrows(SSLException.class,
                () -> tls.write(new ByteBuffer[]{ByteBuffer.wrap("PING\r\n".getBytes(UTF_8))}));
        }
    }

    @Test
    public void testConcurrentHandshake() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tls.conf", false)) {
            URI uri = new URI(ts.getURI());
            NatsChannel socket = SocketNatsChannel.factory().connect(uri, Duration.ofSeconds(2));
            ByteBuffer readBuffer = ByteBuffer.allocate(1024 * 1024);
            socket.read(readBuffer);

            int numThreads = 10;
            SSLEngine sslEngine = createSSLEngine(uri);
            TLSByteChannel tls = new TLSByteChannel(socket, sslEngine);

            CountDownLatch threadsReady = new CountDownLatch(numThreads);
            CountDownLatch startLatch = new CountDownLatch(1);
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            Future<Void>[] futures = new Future[numThreads];
            for (int i = 0; i < 10; i++) {
                boolean isRead = i % 2 == 0;
                futures[i] = executor.submit(() -> {
                    threadsReady.countDown();
                    startLatch.await();
                    if (isRead) {
                        tls.read(EMPTY);
                    } else {
                        tls.write(EMPTY);
                    }
                    return null;
                });
            }

            threadsReady.await();
            startLatch.countDown();

            // Make sure no exception happend on any thread:
            for (int i=0; i < 10; i++) {
                futures[i].get();
            }

            write(tls, "CONNECT {}\r\n");

            readBuffer.clear();
            tls.read(readBuffer);
            readBuffer.flip();
            assertEquals(ByteBuffer.wrap("+OK\r\n".getBytes(UTF_8)), readBuffer);

            tls.close();
        }
    }

    @Test
    public void testConcurrentClose() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tls.conf", false)) {
            URI uri = new URI(ts.getURI());
            NatsChannel socket = SocketNatsChannel.factory().connect(uri, Duration.ofSeconds(2));
            ByteBuffer readBuffer = ByteBuffer.allocate(1024 * 1024);
            socket.read(readBuffer);

            int numThreads = 10;
            SSLEngine sslEngine = createSSLEngine(uri);
            TLSByteChannel tls = new TLSByteChannel(socket, sslEngine);
            tls.handshake();

            CountDownLatch threadsReady = new CountDownLatch(numThreads);
            CountDownLatch startLatch = new CountDownLatch(1);
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            Future<Void>[] futures = new Future[numThreads];
            for (int i = 0; i < 10; i++) {
                futures[i] = executor.submit(() -> {
                    threadsReady.countDown();
                    startLatch.await();
                    tls.close();
                    return null;
                });
            }

            threadsReady.await();
            startLatch.countDown();

            // Make sure no exception happend on any thread:
            for (int i=0; i < 10; i++) {
                futures[i].get();
            }
        }
    }

    @Test
    public void testShortNetRead() throws Exception {
        // Scenario: Net read TLS frame which is larger than the read buffer.
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tls.conf", false)) {
            URI uri = new URI(ts.getURI());
            NatsChannel socket = SocketNatsChannel.factory().connect(uri, Duration.ofSeconds(2));

            AtomicBoolean readOneByteAtATime = new AtomicBoolean(true);
            NatsChannel wrapper = new AbstractNatsChannel(socket) {
                ByteBuffer readBuffer = ByteBuffer.allocate(1);

                @Override
                public int read(ByteBuffer dst) throws IOException {
                    if (!readOneByteAtATime.get()) {
                        return socket.read(dst);
                    }
                    readBuffer.clear();
                    int result = socket.read(readBuffer);
                    if (result <= 0) {
                        return result;
                    }
                    readBuffer.flip();
                    dst.put(readBuffer);
                    return result;
                }

                @Override
                public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
                    return socket.write(srcs, offset, length);
                }

                @Override
                public boolean isSecure() {
                    return false;
                }

                @Override
                public String transformConnectUrl(String connectUrl) {
                    return connectUrl;
                }

            };

            ByteBuffer info = ByteBuffer.allocate(1024 * 1024);
            socket.read(info);

            TLSByteChannel tls = new TLSByteChannel(wrapper, createSSLEngine(uri));

            // Peform handshake:
            tls.read(ByteBuffer.allocate(0));

            // Send connect & ping, but turn off one-byte at a time for readint PONG:
            readOneByteAtATime.set(false);
            write(tls, "CONNECT {}\r\nPING\r\n");

            info.clear();
            tls.read(info);
            info.flip();

            assertEquals(
                ByteBuffer.wrap(
                    "+OK\r\nPONG\r\n"
                    .getBytes(UTF_8)),
                info);

            tls.close();
        }
    }

    private static SSLEngine createSSLEngine(URI uri) throws Exception {
        SSLContext ctx = TestSSLUtils.createTestSSLContext();

        SSLEngine engine = ctx.createSSLEngine(uri.getHost(), uri.getPort());
        engine.setUseClientMode(true);

        return engine;
    }

    private static void write(ByteChannel channel, String str) throws IOException {
        channel.write(ByteBuffer.wrap(str.getBytes(UTF_8)));
    }
}