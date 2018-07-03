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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.NatsServerProtocolMock;
import io.nats.client.NatsTestServer;
import io.nats.client.Options;
import io.nats.client.Subscription;
import io.nats.client.TestHandler;
import io.nats.client.ConnectionListener.Events;

public class ReconnectTests {

    static void flushAndWait(Connection nc, TestHandler handler) {
        try {
            nc.flush(Duration.ofMillis(1000));
        } catch (Exception exp) {
        }

        handler.waitForStatusChange(2000, TimeUnit.MILLISECONDS);
    }

    void checkReconnectingStatus(Connection nc) {
        Connection.Status status = nc.getStatus();
        assertTrue("Reconnecting status", Connection.Status.RECONNECTING == status ||
                                            Connection.Status.DISCONNECTED == status);
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
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

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
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

                end = System.nanoTime();

                assertTrue("reconnect wait", 1_000_000 * (end-start) > 1000);

                // Make sure dispatcher and subscription are still there
                Future<Message> inc = nc.request("dispatchSubject", "test".getBytes(StandardCharsets.UTF_8));
                Message msg = inc.get(500, TimeUnit.MILLISECONDS);
                assertNotNull(msg);

                // make sure the subscription survived
                nc.publish("subsubject", null);
                msg = sub.nextMessage(Duration.ofMillis(100));
                assertNotNull(msg);
            }

            assertEquals("reconnect count", 1, nc.getNatsStatistics().getReconnects());
            assertTrue("exception count", nc.getNatsStatistics().getExceptions() > 0);
        } finally {
            if (nc != null) {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
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
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
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
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

                // Make sure dispatcher and subscription are still there
                Future<Message> inc = nc.request("dispatchSubject", "test".getBytes(StandardCharsets.UTF_8));
                Message msg = inc.get();
                assertNotNull(msg);

                // make sure the subscription survived
                nc.publish("subsubject", null);
                msg = sub.nextMessage(Duration.ofMillis(100));
                assertNotNull(msg);
            }

            assertEquals("reconnect count", 1, nc.getNatsStatistics().getReconnects());
            assertTrue("exception count", nc.getNatsStatistics().getExceptions() > 0);
        } finally {
            if (nc != null) {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
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
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                handler.prepForStatusChange(Events.CLOSED);
            }

            flushAndWait(nc, handler);
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
        } finally {
            if (nc != null) {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
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
                                            connectionListener(handler).
                                            maxReconnects(-1).
                                            build();
                nc = (NatsConnection) Nats.connect(options);
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                assertEquals(nc.getCurrentServerURI(), ts2.getURI());
                handler.prepForStatusChange(Events.RECONNECTED);
            }

            flushAndWait(nc, handler);

            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            assertEquals(nc.getCurrentServerURI(), ts.getURI());
        } finally {
            if (nc != null) {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
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
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                assertEquals(nc.getCurrentServerURI(), ts2.getURI());
                handler.prepForStatusChange(Events.RECONNECTED);
            }

            flushAndWait(nc, handler);

            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            assertEquals(ts.getURI(), nc.getCurrentServerURI());
        } finally {
            if (nc != null) {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testReconnectToSecondServerFromInfo() throws Exception {
        NatsConnection nc = null;
        TestHandler handler = new TestHandler();

        try (NatsTestServer ts = new NatsTestServer()) {
            String customInfo = "{\"server_id\":\"myid\",\"connect_urls\": [\""+ts.getURI()+"\"]}";
            try (NatsServerProtocolMock ts2 = new NatsServerProtocolMock(null, customInfo)) {
                Options options = new Options.Builder().
                                            server(ts2.getURI()).
                                            connectionListener(handler).
                                            maxReconnects(-1).
                                            build();
                nc = (NatsConnection) Nats.connect(options);
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                assertEquals(nc.getCurrentServerURI(), ts2.getURI());
                handler.prepForStatusChange(Events.RECONNECTED);
            }

            flushAndWait(nc, handler);

            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            assertEquals(nc.getCurrentServerURI(), ts.getURI());
        } finally {
            if (nc != null) {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test(expected=IllegalStateException.class)
    public void testOverflowReconnectBuffer() throws Exception {
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
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
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
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
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
}