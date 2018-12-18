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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

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
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            
            Dispatcher d = nc.createDispatcher((msg) -> {
                assertTrue(msg.getReplyTo().startsWith(Options.DEFAULT_INBOX_PREFIX));
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
    public void testSafeRequest() throws IOException, ExecutionException, TimeoutException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            
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
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            
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
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            
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
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            
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
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                
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
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testRequireCleanupOnCancel() throws IOException, ExecutionException, TimeoutException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server(ts.getURI()).requestCleanupInterval(Duration.ofHours(1)).build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                
                Future<Message> incoming = nc.request("subject", null);
                incoming.cancel(true);

                assertEquals(1, ((NatsStatistics)nc.getStatistics()).getOutstandingRequests());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
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
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                
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
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
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
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                
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
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
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
                    assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                    
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
                    assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
                }
            }
    }

    @Test
    public void testOldStyleRequest() throws IOException, ExecutionException, TimeoutException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server(ts.getURI()).oldRequestStyle().build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                
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
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
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
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                
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
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }
    
    @Test(expected = IllegalStateException.class)
    public void throwsIfClosed() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            nc.close();
            nc.request("subject", null);
            assertFalse(true);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testThrowsWithoutSubject() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            nc.request(null, null);
            assertFalse(true);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testThrowsEmptySubject() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            nc.request("", null);
            assertFalse(true);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testThrowsIfTooBig() throws IOException, InterruptedException {
        String customInfo = "{\"server_id\":\"myid\",\"max_payload\":512}";

        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(null, customInfo);
                Connection nc = Nats.connect(ts.getURI())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            assertEquals("got custom info", "myid", ((NatsConnection) nc).getInfo().getServerId());
            assertEquals("got custom info", 512, ((NatsConnection) nc).getInfo().getMaxPayload());
            
            byte[] body = new byte[513];
            nc.request("subject", body);
            assertFalse(true);
        }
    }

}