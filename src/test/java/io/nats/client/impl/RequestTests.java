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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.NatsServerProtocolMock;
import io.nats.client.NatsTestServer;
import io.nats.client.Options;
import io.nats.client.Subscription;

public class RequestTests {
    @Test
    public void testSimpleRequest() throws IOException, ExecutionException, TimeoutException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build())) {
            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
            
            Dispatcher d = nc.createDispatcher((msg) -> {
                assertTrue(msg.getReplyTo().startsWith(StandardCharsets.US_ASCII.decode(Options.DEFAULT_INBOX_PREFIX.asReadOnlyBuffer()).toString()));
                nc.publish(msg.getReplyTo(), null);
            });
            d.subscribe("subject");

            Future<Message> incoming = nc.request("subject", null);
            Message msg = incoming.get(500, TimeUnit.MILLISECONDS);

            assertEquals(0, ((NatsStatistics)nc.getStatistics()).getOutstandingRequests());
            assertNotNull(msg);
            assertEquals(0, msg.getData().length);
            assertTrue(msg.getSubject().indexOf('.') < msg.getSubject().lastIndexOf('.'));
        }
    }

    @Test
    public void testSimpleResponseMessageHasConnection() throws IOException, ExecutionException, TimeoutException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build())) {
            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
            
            Dispatcher d = nc.createDispatcher((msg) -> {
                assertTrue(msg.getReplyTo().startsWith(StandardCharsets.US_ASCII.decode(Options.DEFAULT_INBOX_PREFIX.asReadOnlyBuffer()).toString()));
                msg.getConnection().publish(msg.getReplyTo(), null);
            });
            d.subscribe("subject");

            Future<Message> incoming = nc.request("subject", null);
            Message msg = incoming.get(5000, TimeUnit.MILLISECONDS);

            assertEquals(0, ((NatsStatistics)nc.getStatistics()).getOutstandingRequests());
            assertNotNull(msg);
            assertEquals(0, msg.getData().length);
            assertTrue(msg.getSubject().indexOf('.') < msg.getSubject().lastIndexOf('.'));
            assertTrue(msg.getConnection() == nc);
        }
    }

    @Test
    public void testSafeRequest() throws IOException, ExecutionException, TimeoutException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build())) {
            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
            
            Dispatcher d = nc.createDispatcher((msg) -> {
                nc.publish(msg.getReplyTo(), null);
            });
            d.subscribe("subject");

            Message msg = nc.request("subject", null, Duration.ofMillis(1000));

            assertEquals(0, ((NatsStatistics)nc.getStatistics()).getOutstandingRequests());
            assertNotNull(msg);
            assertEquals(0, msg.getData().length);
            assertTrue(msg.getSubject().indexOf('.') < msg.getSubject().lastIndexOf('.'));
        }
    }

    @Test
    public void testMultipleRequest() throws IOException, ExecutionException, TimeoutException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build())) {
            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
            
            Dispatcher d = nc.createDispatcher((msg) -> {
                nc.publish(msg.getReplyTo(), new byte[7]);
            });
            d.subscribe("subject");

            for (int i=0; i<10; i++) {
                Future<Message> incoming = nc.request("subject", new byte[11]);
                Message msg = incoming.get(500, TimeUnit.MILLISECONDS);

                assertEquals(0, ((NatsStatistics)nc.getStatistics()).getOutstandingRequests());
                assertNotNull(msg);
                assertEquals(7, msg.getData().length);
                assertTrue(msg.getSubject().indexOf('.') < msg.getSubject().lastIndexOf('.'));
            }
        }
    }


    @Test
    public void testManualRequestReply() throws IOException, ExecutionException, TimeoutException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
            
            Dispatcher d = nc.createDispatcher((msg) -> {
                nc.publish(msg.getReplyTo(), msg.getData());
            });
            d.subscribe("request");

            Subscription sub = nc.subscribe("reply");

            nc.publish("request", "reply", "hello".getBytes(StandardCharsets.UTF_8));

            Message msg = sub.nextMessage(Duration.ofMillis(400));

            assertNotNull(msg);
            assertEquals("hello", new String(msg.getData(), StandardCharsets.UTF_8));
        }
    }

    @Test
    public void testRequestWithCustomInbox() throws IOException, ExecutionException, TimeoutException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(new Options.Builder().inboxPrefix("myinbox").server(ts.getURI()).maxReconnects(0).build())) {
            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
            
            Dispatcher d = nc.createDispatcher((msg) -> {
                assertTrue(msg.getReplyTo().startsWith("myinbox"));
                nc.publish(msg.getReplyTo(), null);
            });
            d.subscribe("subject");

            Future<Message> incoming = nc.request("subject", null);
            Message msg = incoming.get(500, TimeUnit.MILLISECONDS);

            assertEquals(0, ((NatsStatistics)nc.getStatistics()).getOutstandingRequests());
            assertNotNull(msg);
            assertEquals(0, msg.getData().length);
            assertTrue(msg.getSubject().indexOf('.') < msg.getSubject().lastIndexOf('.'));
        }
    }

    @Test
    public void testRequireCleanupOnTimeout() throws IOException, ExecutionException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server(ts.getURI()).requestCleanupInterval(Duration.ofHours(1)).build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
                
                Future<Message> incoming = nc.request("subject", null);
                Message msg = null;
                
                try {
                    msg = incoming.get(100, TimeUnit.MILLISECONDS);
                    assertFalse(true);
                } catch(TimeoutException e) {
                    assertTrue(true);
                }

                assertNull(msg);
                assertEquals(1, ((NatsStatistics)nc.getStatistics()).getOutstandingRequests());
            } finally {
                nc.close();
                assertTrue(Connection.Status.CLOSED == nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testRequireCleanupOnCancel() throws IOException, ExecutionException, TimeoutException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server(ts.getURI()).requestCleanupInterval(Duration.ofHours(1)).build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
                
                Future<Message> incoming = nc.request("subject", null);
                incoming.cancel(true);

                assertEquals(1, ((NatsStatistics)nc.getStatistics()).getOutstandingRequests());
            } finally {
                nc.close();
                assertTrue(Connection.Status.CLOSED == nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testCleanupTimerWorks() throws IOException, ExecutionException, TimeoutException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            long cleanupInterval = 50;
            Options options = new Options.Builder().server(ts.getURI()).requestCleanupInterval(Duration.ofMillis(cleanupInterval)).build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
                
                Future<Message> incoming = nc.request("subject", null);
                incoming.cancel(true);
                incoming = nc.request("subject", null);
                incoming.cancel(true);
                incoming = nc.request("subject", null);
                incoming.cancel(true);

                Thread.sleep(2 * cleanupInterval);
                assertEquals(0, ((NatsStatistics)nc.getStatistics()).getOutstandingRequests());

                // Make sure it is still running
                incoming = nc.request("subject", null);
                incoming.cancel(true);
                incoming = nc.request("subject", null);
                incoming.cancel(true);
                incoming = nc.request("subject", null);
                incoming.cancel(true);

                Thread.sleep(2 * cleanupInterval);
                assertEquals(0, ((NatsStatistics)nc.getStatistics()).getOutstandingRequests());
            } finally {
                nc.close();
                assertTrue(Connection.Status.CLOSED == nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testRequestsVsCleanup() throws IOException, ExecutionException, TimeoutException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            long cleanupInterval = 50;
            int msgCount = 100;
            Options options = new Options.Builder().server(ts.getURI()).requestCleanupInterval(Duration.ofMillis(cleanupInterval)).build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
                
                Dispatcher d = nc.createDispatcher((msg) -> {
                    nc.publish(msg.getReplyTo(), null);
                });
                d.subscribe("subject");

                long start = System.nanoTime();
                long end = start;

                while ((end-start) <= 2 * cleanupInterval * 1_000_000) {
                    for (int i=0;i<msgCount;i++) {
                        Future<Message> incoming = nc.request("subject", null);
                        Message msg = incoming.get(500, TimeUnit.MILLISECONDS);
                        assertNotNull(msg);
                        assertEquals(0, msg.getData().length);
                    }
                    end = System.nanoTime();
                }

                assertTrue((end-start) > 2 * cleanupInterval * 1_000_000);
                assertTrue(0 >= ((NatsStatistics)nc.getStatistics()).getOutstandingRequests());
            } finally {
                nc.close();
                assertTrue(Connection.Status.CLOSED == nc.getStatus(), "Closed Status");
            }
        }
    }

        @Test
        public void testDelayInPickingUpFuture() throws IOException, ExecutionException, TimeoutException, InterruptedException {
            try (NatsTestServer ts = new NatsTestServer(false)) {
                int msgCount = 100;
                ArrayList<Future<Message>> messages = new ArrayList<>();
                Connection nc = Nats.connect(ts.getURI());
                try {
                    assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
                    
                    Dispatcher d = nc.createDispatcher((msg) -> {
                        nc.publish(msg.getReplyTo(), new byte[1]);
                    });
                    d.subscribe("subject");
        
                    for (int i=0;i<msgCount;i++) {
                        Future<Message> incoming = nc.request("subject", null);
                        messages.add(incoming);
                    }
                    nc.flush(Duration.ofMillis(1000));

                    for (Future<Message> f : messages) {
                        Message msg = f.get(1000, TimeUnit.MILLISECONDS);
                        assertNotNull(msg);
                        assertEquals(1, msg.getData().length);
                    }
        
                    assertEquals(0, ((NatsStatistics)nc.getStatistics()).getOutstandingRequests());
                } finally {
                    nc.close();
                    assertTrue(Connection.Status.CLOSED == nc.getStatus(), "Closed Status");
                }
            }
    }

    @Test
    public void testOldStyleRequest() throws IOException, ExecutionException, TimeoutException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server(ts.getURI()).oldRequestStyle().build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
                
                Dispatcher d = nc.createDispatcher((msg) -> {
                    nc.publish(msg.getReplyTo(), null);
                });
                d.subscribe("subject");

                Future<Message> incoming = nc.request("subject", null);
                Message msg = incoming.get(500, TimeUnit.MILLISECONDS);

                assertEquals(0, ((NatsStatistics)nc.getStatistics()).getOutstandingRequests());
                assertNotNull(msg);
                assertEquals(0, msg.getData().length);
                assertTrue(msg.getSubject().indexOf('.') == msg.getSubject().lastIndexOf('.'));
            } finally {
                nc.close();
                assertTrue(Connection.Status.CLOSED == nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testBuffersResize() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            int initialSize = 128;
            int messageSize = 1024;
            Options options = new Options.Builder().
                                    server(ts.getURI()).
                                    bufferSize(initialSize).
                                    connectionTimeout(Duration.ofSeconds(10)).
                                    build();

            Connection nc = Nats.connect(options);
            try {
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
                
                Dispatcher d = nc.createDispatcher((msg) -> {
                    nc.publish(msg.getReplyTo(), msg.getData());
                });
                d.subscribe("subject");

                Future<Message> incoming = nc.request("subject", new byte[messageSize]); // force the buffers to resize
                Message msg = null;

                try {
                    msg = incoming.get(30000, TimeUnit.MILLISECONDS);
                } catch (Exception exp) {
                    exp.printStackTrace();
                }

                assertEquals(0, ((NatsStatistics)nc.getStatistics()).getOutstandingRequests());
                assertNotNull(msg);
                assertEquals(messageSize, msg.getData().length);
            } finally {
                nc.close();
                assertTrue(Connection.Status.CLOSED == nc.getStatus(), "Closed Status");
            }
        }
    }
    
    @Test
    public void throwsIfClosed() {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                nc.close();
                nc.request("subject", null);
                assertFalse(true);
            }
        });
    }

    @Test
    public void testThrowsWithoutSubject() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
                nc.request(null, null);
                assertFalse(true);
            }
        });
    }

    @Test
    public void testThrowsEmptySubject() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
                nc.request("", null);
                assertFalse(true);
            }
        });
    }

    @Test
    public void testThrowsIfTooBig() {
        assertThrows(IllegalArgumentException.class, () -> {
            String customInfo = "{\"server_id\":\"myid\",\"max_payload\":512}";

            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(null, customInfo);
                    Connection nc = Nats.connect(ts.getURI())) {
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
                assertEquals("myid", ((NatsConnection) nc).getInfo().getServerId(), "got custom info");
                assertEquals(512, ((NatsConnection) nc).getInfo().getMaxPayload(), "got custom info");

                byte[] body = new byte[513];
                nc.request("subject", body);
                assertFalse(true);
            }
        });
    }

}