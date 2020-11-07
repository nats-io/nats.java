// Copyright 2015-2018 The NATS Authors
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.NatsServerProtocolMock;
import io.nats.client.NatsTestServer;
import io.nats.client.Options;
import io.nats.client.Subscription;
import io.nats.client.TestHandler;
import io.nats.client.TestSSLUtils;
import io.nats.client.ConnectionListener.Events;

import javax.net.ssl.SSLContext;

public class ReconnectTests {

    static void flushAndWait(Connection nc, TestHandler handler) {
        try {
            nc.flush(Duration.ofSeconds(2));
        } catch (Exception exp) {
        }

        handler.waitForStatusChange(15, TimeUnit.SECONDS);
    }

    void checkReconnectingStatus(Connection nc) {
        Connection.Status status = nc.getStatus();
        assertTrue(Connection.Status.RECONNECTING == status ||
                                            Connection.Status.DISCONNECTED == status, "Reconnecting status");
    }

    @Test
    public void testSimpleReconnect() throws Exception { //Includes test for subscriptions and dispatchers across reconnect
        NatsConnection nc = null;
        TestHandler handler = new TestHandler();
        int port = NatsTestServer.nextPort();
        Subscription sub;
        long start = 0;
        long end = 0;

        handler.setPrintExceptions(true);

        try {
            try (NatsTestServer ts = new NatsTestServer(port, false)) {
                Options options = new Options.Builder().
                                    server(ts.getURI()).
                                    maxReconnects(-1).
                                    reconnectWait(Duration.ofMillis(1000)).
                                    connectionListener(handler).
                                    build();
                                    port = ts.getPort();
                nc = (NatsConnection) Nats.connect(options);
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

                sub = nc.subscribe("subsubject");
                
                final NatsConnection nnc = nc;
                Dispatcher d = nc.createDispatcher((msg) -> {
                    nnc.publish(msg.getReplyTo(), msg.getData());
                });
                d.subscribe("dispatchSubject");
                nc.flush(Duration.ofMillis(1000));

                Future<Message> inc = nc.request("dispatchSubject", "test".getBytes(StandardCharsets.UTF_8));
                Message msg = inc.get();
                assertNotNull(msg);

                nc.publish("subsubject", null);
                msg = sub.nextMessage(Duration.ofMillis(100));
                assertNotNull(msg);

                handler.prepForStatusChange(Events.DISCONNECTED);
                start = System.nanoTime();
            }

            flushAndWait(nc, handler);
            checkReconnectingStatus(nc);

            handler.prepForStatusChange(Events.RESUBSCRIBED);

            try (NatsTestServer ts = new NatsTestServer(port, false)) {
                handler.waitForStatusChange(5000, TimeUnit.MILLISECONDS);
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

                end = System.nanoTime();

                assertTrue(1_000_000 * (end-start) > 1000, "reconnect wait");

                // Make sure dispatcher and subscription are still there
                Future<Message> inc = nc.request("dispatchSubject", "test".getBytes(StandardCharsets.UTF_8));
                Message msg = inc.get(500, TimeUnit.MILLISECONDS);
                assertNotNull(msg);

                // make sure the subscription survived
                nc.publish("subsubject", null);
                msg = sub.nextMessage(Duration.ofMillis(100));
                assertNotNull(msg);
            }

            assertEquals(1, nc.getNatsStatistics().getReconnects(), "reconnect count");
            assertTrue(nc.getNatsStatistics().getExceptions() > 0, "exception count");
        } finally {
            if (nc != null) {
                nc.close();
                assertTrue(Connection.Status.CLOSED == nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testSubscribeDuringReconnect() throws Exception {
        NatsConnection nc = null;
        TestHandler handler = new TestHandler();
        int port;
        Subscription sub;

        try {
            try (NatsTestServer ts = new NatsTestServer()) {
                Options options = new Options.Builder().
                                    server(ts.getURI()).
                                    maxReconnects(-1).
                                    reconnectWait(Duration.ofMillis(20)).
                                    connectionListener(handler).
                                    build();
                                    port = ts.getPort();
                nc = (NatsConnection) Nats.connect(options);
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
                handler.prepForStatusChange(Events.DISCONNECTED);
            }

            flushAndWait(nc, handler);
            checkReconnectingStatus(nc);

            sub = nc.subscribe("subsubject");
                
            final NatsConnection nnc = nc;
            Dispatcher d = nc.createDispatcher((msg) -> {
                nnc.publish(msg.getReplyTo(), msg.getData());
            });
            d.subscribe("dispatchSubject");

            handler.prepForStatusChange(Events.RECONNECTED);

            try (NatsTestServer ts = new NatsTestServer(port, false)) {
                handler.waitForStatusChange(400, TimeUnit.MILLISECONDS);
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

                // Make sure dispatcher and subscription are still there
                Future<Message> inc = nc.request("dispatchSubject", "test".getBytes(StandardCharsets.UTF_8));
                Message msg = inc.get();
                assertNotNull(msg);

                // make sure the subscription survived
                nc.publish("subsubject", null);
                msg = sub.nextMessage(Duration.ofMillis(100));
                assertNotNull(msg);
            }

            assertEquals(1, nc.getNatsStatistics().getReconnects(), "reconnect count");
            assertTrue(nc.getNatsStatistics().getExceptions() > 0, "exception count");
        } finally {
            if (nc != null) {
                nc.close();
                assertTrue(Connection.Status.CLOSED == nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testReconnectBuffer() throws Exception {
        NatsConnection nc = null;
        TestHandler handler = new TestHandler();
        int port = NatsTestServer.nextPort();
        Subscription sub;
        long start = 0;
        long end = 0;
        String[] customArgs = {"--user","stephen","--pass","password"};

        handler.setPrintExceptions(true);

        try {
            try (NatsTestServer ts = new NatsTestServer(customArgs, port, false)) {
                Options options = new Options.Builder().
                                    server(ts.getURI()).
                                    maxReconnects(-1).
                                    userInfo("stephen".toCharArray(), "password".toCharArray()).
                                    reconnectWait(Duration.ofMillis(1000)).
                                    connectionListener(handler).
                                    build();
                nc = (NatsConnection) Nats.connect(options);
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

                sub = nc.subscribe("subsubject");
                
                final NatsConnection nnc = nc;
                Dispatcher d = nc.createDispatcher((msg) -> {
                    nnc.publish(msg.getReplyTo(), msg.getData());
                });
                d.subscribe("dispatchSubject");
                nc.flush(Duration.ofMillis(1000));

                Future<Message> inc = nc.request("dispatchSubject", "test".getBytes(StandardCharsets.UTF_8));
                Message msg = inc.get();
                assertNotNull(msg);

                nc.publish("subsubject", null);
                msg = sub.nextMessage(Duration.ofMillis(100));
                assertNotNull(msg);

                handler.prepForStatusChange(Events.DISCONNECTED);
                start = System.nanoTime();
            }

            flushAndWait(nc, handler);
            checkReconnectingStatus(nc);

            // Send a message to the dispatcher and one to the subscriber
            // These should be sent on reconnect
            Future<Message> inc = nc.request("dispatchSubject", "test".getBytes(StandardCharsets.UTF_8));
            nc.publish("subsubject", null);
            nc.publish("subsubject", null);

            handler.prepForStatusChange(Events.RESUBSCRIBED);

            try (NatsTestServer ts = new NatsTestServer(customArgs, port, false)) {
                handler.waitForStatusChange(5000, TimeUnit.MILLISECONDS);
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

                end = System.nanoTime();

                assertTrue(1_000_000 * (end-start) > 1000, "reconnect wait");

                // Check the message we sent to dispatcher
                Message msg = inc.get(500, TimeUnit.MILLISECONDS);
                assertNotNull(msg);

                // Check the two we sent to subscriber
                msg = sub.nextMessage(Duration.ofMillis(500));
                assertNotNull(msg);

                msg = sub.nextMessage(Duration.ofMillis(500));
                assertNotNull(msg);
            }

            assertEquals(1, nc.getNatsStatistics().getReconnects(), "reconnect count");
            assertTrue(nc.getNatsStatistics().getExceptions() > 0, "exception count");
        } finally {
            if (nc != null) {
                nc.close();
                assertTrue(Connection.Status.CLOSED == nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testMaxReconnects() throws Exception {
        Connection nc = null;
        TestHandler handler = new TestHandler();
        int port = NatsTestServer.nextPort();

        try {
            try (NatsTestServer ts = new NatsTestServer(port, false)) {
                Options options = new Options.Builder().
                                    server(ts.getURI()).
                                    maxReconnects(1).
                                    connectionListener(handler).
                                    reconnectWait(Duration.ofMillis(10)).
                                    build();
                nc = Nats.connect(options);
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
                handler.prepForStatusChange(Events.CLOSED);
            }

            flushAndWait(nc, handler);
            assertTrue(Connection.Status.CLOSED == nc.getStatus(), "Closed Status");
        } finally {
            if (nc != null) {
                nc.close();
                assertTrue(Connection.Status.CLOSED == nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testReconnectToSecondServer() throws Exception {
        NatsConnection nc = null;
        TestHandler handler = new TestHandler();

        try (NatsTestServer ts = new NatsTestServer()) {
            try (NatsTestServer ts2 = new NatsTestServer()) {
                Options options = new Options.Builder().
                                            server(ts2.getURI()).
                                            server(ts.getURI()).
                                            noRandomize().
                                            connectionListener(handler).
                                            maxReconnects(-1).
                                            build();
                nc = (NatsConnection) Nats.connect(options);
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
                assertEquals(ts2.getURI(), nc.getConnectedUrl());
                handler.prepForStatusChange(Events.RECONNECTED);
            }

            flushAndWait(nc, handler);

            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
            assertEquals(ts.getURI(), nc.getConnectedUrl());
        } finally {
            if (nc != null) {
                nc.close();
                assertTrue(Connection.Status.CLOSED == nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testNoRandomizeReconnectToSecondServer() throws Exception {
        NatsConnection nc = null;
        TestHandler handler = new TestHandler();

        try (NatsTestServer ts = new NatsTestServer()) {
            try (NatsTestServer ts2 = new NatsTestServer()) {
                Options options = new Options.Builder().
                                            server(ts2.getURI()).
                                            server(ts.getURI()).
                                            connectionListener(handler).
                                            maxReconnects(-1).
                                            noRandomize().
                                            build();
                nc = (NatsConnection) Nats.connect(options);
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
                assertEquals(nc.getConnectedUrl(), ts2.getURI());
                handler.prepForStatusChange(Events.RECONNECTED);
            }

            flushAndWait(nc, handler);

            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
            assertEquals(ts.getURI(), nc.getConnectedUrl());
        } finally {
            if (nc != null) {
                nc.close();
                assertTrue(Connection.Status.CLOSED == nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testReconnectToSecondServerFromInfo() throws Exception {
        NatsConnection nc = null;
        TestHandler handler = new TestHandler();

        try (NatsTestServer ts = new NatsTestServer()) {
            String striped = ts.getURI().substring("nats://".length()); // info doesn't have protocol
            String customInfo = "{\"server_id\":\"myid\",\"connect_urls\": [\""+striped+"\"]}";
            try (NatsServerProtocolMock ts2 = new NatsServerProtocolMock(null, customInfo)) {
                Options options = new Options.Builder().
                                            server(ts2.getURI()).
                                            connectionListener(handler).
                                            maxReconnects(-1).
                                            connectionTimeout(Duration.ofSeconds(5)).
                                            reconnectWait(Duration.ofSeconds(1)).
                                            build();
                nc = (NatsConnection) Nats.connect(options);
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
                assertEquals(nc.getConnectedUrl(), ts2.getURI());
                handler.prepForStatusChange(Events.RECONNECTED);
            }

            flushAndWait(nc, handler);

            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
            assertTrue(ts.getURI().endsWith(nc.getConnectedUrl()));
        } finally {
            if (nc != null) {
                nc.close();
                assertTrue(Connection.Status.CLOSED == nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testOverflowReconnectBuffer() {
        assertThrows(IllegalStateException.class, () -> {
            Connection nc = null;
            TestHandler handler = new TestHandler();

            try {
                try (NatsTestServer ts = new NatsTestServer()) {
                    Options options = new Options.Builder().
                                            server(ts.getURI()).
                                            maxReconnects(-1).
                                            connectionListener(handler).
                                            reconnectBufferSize(4*512).
                                            reconnectWait(Duration.ofSeconds(480)).
                                            build();
                    nc = Nats.connect(options);
                    assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
                    handler.prepForStatusChange(Events.DISCONNECTED);
                }

                flushAndWait(nc, handler);
                checkReconnectingStatus(nc);

                for (int i=0;i<20;i++) {
                    nc.publish("test", new byte[512]);// Should blow up by the 5th message
                }

                assertFalse(true);
            } finally {
                if (nc != null) {
                    nc.close();
                }
            }
        });
    }

    @Test
    public void testInfiniteReconnectBuffer() throws Exception {
        Connection nc = null;
        TestHandler handler = new TestHandler();
        handler.setPrintExceptions(false);
        
        try {
            try (NatsTestServer ts = new NatsTestServer()) {
                Options options = new Options.Builder().
                                        server(ts.getURI()).
                                        maxReconnects(5).
                                        connectionListener(handler).
                                        reconnectBufferSize(-1).
                                        reconnectWait(Duration.ofSeconds(30)).
                                        build();
                nc = Nats.connect(options);
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
                handler.prepForStatusChange(Events.DISCONNECTED);
            }

            flushAndWait(nc, handler);
            checkReconnectingStatus(nc);

            byte[] payload = new byte[1024];
            for (int i=0;i<1_000;i++) {
                nc.publish("test", payload);
            }

            assertTrue(true);
        } finally {
            if (nc != null) {
                nc.close();
            }
        }
    }

    @Test
    public void testReconnectDropOnLineFeed() throws Exception {
        NatsConnection nc = null;
        TestHandler handler = new TestHandler();
        int port = NatsTestServer.nextPort();
        Duration reconnectWait = Duration.ofMillis(100); // thrash
        int thrashCount = 5;
        CompletableFuture<Boolean> gotSub = new CompletableFuture<>();
        AtomicReference<CompletableFuture<Boolean>> subRef = new AtomicReference<>(gotSub);
        CompletableFuture<Boolean> sendMsg = new CompletableFuture<>();
        AtomicReference<CompletableFuture<Boolean>> sendRef = new AtomicReference<>(sendMsg);

        NatsServerProtocolMock.Customizer receiveMessageCustomizer = (ts, r,w) -> {
            String subLine = "";
            
            System.out.println("*** Mock Server @" + ts.getPort() + " waiting for SUB ...");
            try {
                subLine = r.readLine();
            } catch(Exception e) {
                subRef.get().cancel(true);
                return;
            }

            if (subLine.startsWith("SUB")) {
                subRef.get().complete(Boolean.TRUE);
            }

            try {
                sendRef.get().get();
            } catch (Exception e) {
                //keep going
            }

            w.write("MSG\r"); // Drop the line feed
            w.flush();
        };

        try {
            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(receiveMessageCustomizer, port, true)) {
                Options options = new Options.Builder().
                                    server(ts.getURI()).
                                    maxReconnects(-1).
                                    reconnectWait(reconnectWait).
                                    connectionListener(handler).
                                    build();
                                    port = ts.getPort();
                nc = (NatsConnection) Nats.connect(options);
                assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                nc.subscribe("test");
                subRef.get().get();
                handler.prepForStatusChange(Events.DISCONNECTED);
                sendRef.get().complete(true);
                flushAndWait(nc, handler); // mock server will close so we do this inside the curly
            }

            // Thrash in and out of connect status
            // server starts thrashCount times so we should succeed thrashCount x
            for (int i=0;i<thrashCount;i++) {
                checkReconnectingStatus(nc);

                // connect good then bad
                handler.prepForStatusChange(Events.RESUBSCRIBED);
                try (NatsTestServer ts = new NatsTestServer(port, false)) {
                    handler.waitForStatusChange(10, TimeUnit.SECONDS);
                    assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                    handler.prepForStatusChange(Events.DISCONNECTED);
                }

                flushAndWait(nc, handler); // nats won't close until we tell it, so put this outside the curly
                checkReconnectingStatus(nc);

                gotSub = new CompletableFuture<>();
                subRef.set(gotSub);
                sendMsg = new CompletableFuture<>();
                sendRef.set(sendMsg);

                handler.prepForStatusChange(Events.RESUBSCRIBED);
                try (NatsServerProtocolMock ts = new NatsServerProtocolMock(receiveMessageCustomizer, port, true)) {
                    handler.waitForStatusChange(10, TimeUnit.SECONDS);
                    assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                    subRef.get().get();
                    handler.prepForStatusChange(Events.DISCONNECTED);
                    sendRef.get().complete(true);
                    flushAndWait(nc, handler); // mock server will close so we do this inside the curly
                }
            }
            

            assertEquals(2 * thrashCount, nc.getNatsStatistics().getReconnects(), "reconnect count");
        } finally {
            if (nc != null) {
                nc.close();
                assertTrue(Connection.Status.CLOSED == nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testReconnectNoIPTLSConnection() throws Exception {
        NatsConnection nc = null;
        TestHandler handler = new TestHandler();
        NatsTestServer ts = null;
        NatsTestServer ts2 = null;

        try {
            int tsPort = NatsTestServer.nextPort();
            int ts2Port = NatsTestServer.nextPort();
            int tsCPort = NatsTestServer.nextPort();
            int ts2CPort = NatsTestServer.nextPort();

            String[] tsInserts = {
                "",
                "cluster {",
                "listen: localhost:" + tsCPort,
                "routes = [",
                    "nats-route://localhost:" + ts2CPort,
                "]",
                "}"
            };
            String[] ts2Inserts = {
                "cluster {",
                "listen: localhost:" + ts2CPort,
                "routes = [",
                    "nats-route://127.0.0.1:" + tsCPort,
                "]",
                "}"
            };

            // Regular tls for first connection, then no ip for second
            ts = new NatsTestServer("src/test/resources/tls_noip.conf", tsInserts, tsPort, false);
            ts2 = new NatsTestServer("src/test/resources/tls_noip.conf", ts2Inserts, ts2Port, false);

            SSLContext ctx = TestSSLUtils.createTestSSLContext();
            Options options = new Options.Builder().
                                        server(ts.getURI()).
                                        sslContext(ctx).
                                        connectionListener(handler).
                                        maxReconnects(20). // we get multiples for some, so need enough
                                        reconnectWait(Duration.ofMillis(100)).
                                        connectionTimeout(Duration.ofSeconds(5)).
                                        noRandomize().
                                        build();
            
            handler.prepForStatusChange(Events.DISCOVERED_SERVERS);
            nc = (NatsConnection) Nats.connect(options);
            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
            assertEquals(nc.getConnectedUrl(), ts.getURI());

            flushAndWait(nc, handler); // make sure we get the new server via info

            handler.prepForStatusChange(Events.RECONNECTED);

            ts.close();
            flushAndWait(nc, handler);

            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

            URI uri = options.createURIForServer(nc.getConnectedUrl());
            assertEquals(ts2.getPort(), uri.getPort()); // full uri will have some ip address, just check port
        } finally {
            if (nc != null) {
                nc.close();
                assertTrue(Connection.Status.CLOSED == nc.getStatus(), "Closed Status");
            }
            if (ts != null) {
                ts.close();
            }
            if (ts2 != null) {
                ts2.close();
            }
        }
    }

    @Test
    public void testURISchemeNoIPTLSConnection() throws Exception {
        //System.setProperty("javax.net.debug", "all");
        SSLContext ctx = TestSSLUtils.createTestSSLContext();
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tls_noip.conf", false)) {
            Options options = new Options.Builder().
                                server("tls://localhost:"+ts.getPort()).
                                maxReconnects(0).
                                sslContext(ctx).
                                build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
            } finally {
                nc.close();
                assertTrue(Connection.Status.CLOSED == nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testURISchemeNoIPOpenTLSConnection() throws Exception {
        //System.setProperty("javax.net.debug", "all");
        SSLContext ctx = TestSSLUtils.createTestSSLContext();
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tls_noip.conf", false)) {
            Options options = new Options.Builder().
                                server("opentls://localhost:"+ts.getPort()).
                                maxReconnects(0).
                                sslContext(ctx).
                                build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
            } finally {
                nc.close();
                assertTrue(Connection.Status.CLOSED == nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testWriterFilterTiming() throws Exception {
        NatsConnection nc = null;
        TestHandler handler = new TestHandler();
        int port = NatsTestServer.nextPort();

        try {
            try (NatsTestServer ts = new NatsTestServer(port, false)) {
                Options options = new Options.Builder().
                                    server(ts.getURI()).
                                    noReconnect().
                                    connectionListener(handler).
                                    build();
                                    port = ts.getPort();
                nc = (NatsConnection) Nats.connect(options);
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

                for (int i=0;i<100;i++) {
                    // stop and start in a loop without waiting for the future to complete
                    nc.getWriter().stop();
                    nc.getWriter().start(nc.getDataPortFuture());
                }

                nc.getWriter().stop();
                Thread.sleep(1000);
                // Should have thrown an exception if #203 isn't fixed
            }
        } finally {
            if (nc != null) {
                nc.close();
                assertTrue(Connection.Status.CLOSED == nc.getStatus(), "Closed Status");
            }
        }
    }

   
    private class TestReconnecWaitHandler implements ConnectionListener {
        int disconnectCount = 0;

        public synchronized int getDisconnectCount() {
            return disconnectCount;
        }

        private synchronized void incrementDisconnectedCount() {
            disconnectCount++;
        }

        @Override
        public void connectionEvent(Connection conn, Events type) {
            if (type == Events.DISCONNECTED) {
                // disconnect is called after every failed reconnect attempt.
                incrementDisconnectedCount();
            }
        }
    } 
    
    @Test
    public void testReconnectWait() throws IOException, InterruptedException {
        TestReconnecWaitHandler trwh = new TestReconnecWaitHandler();

        int port = NatsTestServer.nextPort();
        Options options = new Options.Builder().
                                server("nats://localhost:"+port).
                                maxReconnects(-1).
                                connectionTimeout(Duration.ofSeconds(1)).
                                reconnectWait(Duration.ofMillis(250)).
                                connectionListener(trwh).
                                build();

        NatsTestServer ts = new NatsTestServer(port, false);
        Connection c = Nats.connect(options);
        ts.close();

        try {
            Thread.sleep(250);
        } catch (Exception exp) {
        }

        assertTrue(trwh.getDisconnectCount() < 3, "disconnectCount");
        
        c.close();
    }  

}