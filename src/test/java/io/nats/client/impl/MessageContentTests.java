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


import io.nats.client.*;
import io.nats.client.ConnectionListener.Events;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.nats.client.utils.TestBase.runInServer;
import static io.nats.client.utils.TestBase.subject;
import static org.junit.jupiter.api.Assertions.*;


public class MessageContentTests {
    @Test
    public void testSimpleString() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
            
            Dispatcher d = nc.createDispatcher((msg) -> {
                nc.publish(msg.getReplyTo(), msg.getData());
            });
            d.subscribe("subject");

            String body = "hello world";
            byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
            Future<Message> incoming = nc.request("subject", bodyBytes);
            Message msg = incoming.get(50000, TimeUnit.MILLISECONDS);

            assertNotNull(msg);
            assertEquals(bodyBytes.length, msg.getData().length);
            assertEquals(body, new String(msg.getData(), StandardCharsets.UTF_8));
        }
    }

    @Test
    public void testUTF8String() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
            
            Dispatcher d = nc.createDispatcher((msg) -> {
                nc.publish(msg.getReplyTo(), msg.getData());
            });
            d.subscribe("subject");

            String body = "??????";
            byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
            Future<Message> incoming = nc.request("subject", bodyBytes);
            Message msg = incoming.get(500, TimeUnit.MILLISECONDS);

            assertNotNull(msg);
            assertEquals(bodyBytes.length, msg.getData().length);
            assertEquals(body, new String(msg.getData(), StandardCharsets.UTF_8));
        }
    }

    @Test
    public void testDifferentSizes() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
            
            Dispatcher d = nc.createDispatcher((msg) -> {
                nc.publish(msg.getReplyTo(), msg.getData());
            });
            d.subscribe("subject");

            String body = "hello world";
            for (int i=0;i<10;i++) {

                byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
                Future<Message> incoming = nc.request("subject", bodyBytes);
                Message msg = incoming.get(500, TimeUnit.MILLISECONDS);

                assertNotNull(msg);
                assertEquals(bodyBytes.length, msg.getData().length);
                assertEquals(body, new String(msg.getData(), StandardCharsets.UTF_8));

                body = body+body;
            }
        }
    }

    @Test
    public void testZeros() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
            
            Dispatcher d = nc.createDispatcher((msg) -> {
                nc.publish(msg.getReplyTo(), msg.getData());
            });
            d.subscribe("subject");

            byte[] data = new byte[17];
            Future<Message> incoming = nc.request("subject", data);
            Message msg = incoming.get(500, TimeUnit.MILLISECONDS);

            assertNotNull(msg);
            assertEquals(data.length, msg.getData().length);
            assertTrue(Arrays.equals(msg.getData(), data));
        }
    }

    @Test
    public void testHeaders() throws Exception {
        runInServer(nc -> {
            CountDownLatch latch = new CountDownLatch(6);
            Dispatcher d = nc.createDispatcher();
            d.subscribe(subject(0), msg -> {
                assertEquals(10, msg.getData().length);
                assertFalse(msg.hasHeaders());
                assertNull(msg.getHeaders());
                latch.countDown();
            });
            d.subscribe(subject(1), msg -> {
                assertEquals(4, msg.getData().length);
                assertTrue(msg.hasHeaders());
                assertNotNull(msg.getHeaders());
                assertEquals("msg-has-data", msg.getHeaders().getFirst("one"));
                latch.countDown();
            });
            d.subscribe(subject(2), msg -> {
                assertEquals(0, msg.getData().length);
                assertTrue(msg.hasHeaders());
                assertNotNull(msg.getHeaders());
                assertEquals("msg-no-data", msg.getHeaders().getFirst("two"));
                latch.countDown();
            });
            d.subscribe(subject(3), msg -> {
                assertTrue(msg.hasHeaders());
                assertNotNull(msg.getHeaders());
                assertEquals("changed", msg.getHeaders().getFirst("three"));
                latch.countDown();
            });
            d.subscribe(subject(4), msg -> {
                assertTrue(msg.hasHeaders());
                assertNotNull(msg.getHeaders());
                assertNull(msg.getHeaders().getFirst("three"));
                assertEquals("also-changed", msg.getHeaders().getFirst("four"));
                latch.countDown();
            });
            d.subscribe(subject(5), msg -> {
                assertEquals("no-headers", new String(msg.getData()));
                assertFalse(msg.hasHeaders());
                latch.countDown();
            });

            nc.publish(NatsMessage.builder()
                .subject(subject(0))
                .data("no-headers".getBytes(StandardCharsets.UTF_8))
                .build());

            nc.publish(NatsMessage.builder()
                .subject(subject(1))
                .data("data".getBytes(StandardCharsets.UTF_8))
                .headers(new Headers().put("one", "msg-has-data"))
                .build());

            nc.publish(NatsMessage.builder()
                .subject(subject(2))
                .headers(new Headers().put("two", "msg-no-data"))
                .build());

            Headers h = new Headers().put("three", "change-after-make-message");
            Message m = NatsMessage.builder()
                .subject(subject(3))
                .headers(h)
                .build();
            h.put("three", "changed");
            nc.publish(m);

            m = NatsMessage.builder()
                .subject(subject(4))
                .headers(h)
                .build();
            h.remove("three");
            h.put("four", "also-changed");
            nc.publish(m);

            m = NatsMessage.builder()
                .subject(subject(5))
                .data("no-headers")
                .headers(h)
                .build();
            h.clear();
            nc.publish(m);

            latch.await(5, TimeUnit.SECONDS); // wait at most 5 seconds, gives time for the messages to round trip
        });
    }

    @Test
    public void testDisconnectOnMissingLineFeedContent() throws Exception {
        CompletableFuture<Boolean> ready = new CompletableFuture<>();
        NatsServerProtocolMock.Customizer badServer = (ts, r, w) -> {

            // Wait for client to be ready.
            try {
                ready.get();
            } catch (Exception e) {
                return;
            }

            System.out.println("*** Mock Server @" + ts.getPort() + " sending bad message ...");
            w.write("MSG test 0 4\rtest"); // Missing \n
            w.flush();
        };

        runBadContentTest(badServer, ready);
    }
    
    @Test
    public void testDisconnectOnTooMuchData() throws Exception {
        CompletableFuture<Boolean> ready = new CompletableFuture<>();
        NatsServerProtocolMock.Customizer badServer = (ts, r, w) -> {

            // Wait for client to be ready.
            try {
                ready.get();
            } catch (Exception e) {
                return;
            }

            System.out.println("*** Mock Server @" + ts.getPort() + " sending bad message ...");
            w.write("MSG test 0 4\r\ntesttesttest"); // data is too long
            w.flush();
        };

        runBadContentTest(badServer, ready);
    }
    
    @Test
    public void testDisconnectOnNoLineFeedAfterData() throws Exception {
        CompletableFuture<Boolean> ready = new CompletableFuture<>();
        NatsServerProtocolMock.Customizer badServer = (ts, r, w) -> {

            // Wait for client to be ready.
            try {
                ready.get();
            } catch (Exception e) {
                return;
            }

            System.out.println("*** Mock Server @" + ts.getPort() + " sending bad message ...");
            w.write("MSG test 0 4\r\ntest\rPING"); // no \n after data
            w.flush();
        };

        runBadContentTest(badServer, ready);
    }
    
    @Test
    public void testDisconnectOnBadProtocol() throws Exception {
        CompletableFuture<Boolean> ready = new CompletableFuture<>();
        NatsServerProtocolMock.Customizer badServer = (ts, r, w) -> {
            // Wait for client to be ready.
            try {
                ready.get();
            } catch (Exception e) {
                return;
            }

            System.out.println("*** Mock Server @" + ts.getPort() + " sending bad message ...");
            w.write("BLAM\r\n"); // Bad protocol op
            w.flush();
        };

        runBadContentTest(badServer, ready);
    }

    void runBadContentTest(NatsServerProtocolMock.Customizer badServer, CompletableFuture<Boolean> ready) throws Exception {
        TestHandler handler = new TestHandler();

        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(badServer, null)) {
            Options options = new Options.Builder().
                                server(ts.getURI()).
                                maxReconnects(0).
                                errorListener(handler).
                                connectionListener(handler).
                                build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

                handler.prepForStatusChange(Events.DISCONNECTED);
                ready.complete(Boolean.TRUE);
                handler.waitForStatusChange(200, TimeUnit.MILLISECONDS);

                assertTrue(handler.getExceptionCount() > 0);
                assertTrue(Connection.Status.DISCONNECTED == nc.getStatus()
                                                    || Connection.Status.CLOSED == nc.getStatus(), "Disconnected Status");
            } finally {
                nc.close();
                assertTrue(Connection.Status.CLOSED == nc.getStatus(), "Closed Status");
            }
        }
    }
}