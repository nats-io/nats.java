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
import io.nats.client.support.NatsRequestCompletableFuture;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class RequestTests extends TestBase {
    @Test
    public void testSimpleRequest() throws IOException, ExecutionException, TimeoutException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build())) {
            assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
            
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
    public void testRequestVarieties() throws Exception {
        runInServer(nc -> {
            Dispatcher d = nc.createDispatcher((msg) -> {
                nc.publish(msg.getReplyTo(), msg.getData());
            });
            d.subscribe(SUBJECT);

            Future<Message> f = nc.request(SUBJECT, dataBytes(1));
            Message msg = f.get(500, TimeUnit.MILLISECONDS);
            assertEquals(data(1), new String(msg.getData()));

            NatsMessage nm = NatsMessage.builder().subject(SUBJECT).data(dataBytes(2)).build();
            f = nc.request(nm);
            msg = f.get(500, TimeUnit.MILLISECONDS);
            assertEquals(data(2), new String(msg.getData()));

            msg = nc.request(SUBJECT, dataBytes(3), Duration.ofSeconds(1));
            assertEquals(data(3), new String(msg.getData()));

            nm = NatsMessage.builder().subject(SUBJECT).data(dataBytes(4)).build();
            msg = nc.request(nm, Duration.ofSeconds(1));
            assertEquals(data(4), new String(msg.getData()));

            assertThrows(IllegalArgumentException.class, () -> nc.request(null));
            assertThrows(IllegalArgumentException.class, () -> nc.request(null, Duration.ofSeconds(1)));
        });
    }

    @Test
    public void testSimpleResponseMessageHasConnection() throws IOException, ExecutionException, TimeoutException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build())) {
            assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
            
            Dispatcher d = nc.createDispatcher((msg) -> {
                assertTrue(msg.getReplyTo().startsWith(Options.DEFAULT_INBOX_PREFIX));
                msg.getConnection().publish(msg.getReplyTo(), null);
            });
            d.subscribe("subject");

            Future<Message> incoming = nc.request("subject", null);
            Message msg = incoming.get(5000, TimeUnit.MILLISECONDS);

            assertEquals(0, ((NatsStatistics)nc.getStatistics()).getOutstandingRequests());
            assertNotNull(msg);
            assertEquals(0, msg.getData().length);
            assertTrue(msg.getSubject().indexOf('.') < msg.getSubject().lastIndexOf('.'));
            assertEquals(msg.getConnection(), nc);
        }
    }

    @Test
    public void testSafeRequest() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build())) {
            assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
            
            Dispatcher d = nc.createDispatcher((msg) -> nc.publish(msg.getReplyTo(), null));
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
            assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
            
            Dispatcher d = nc.createDispatcher((msg) -> nc.publish(msg.getReplyTo(), new byte[7]));
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
    public void testMultipleReplies() throws Exception {
        Options.Builder builder = new Options.Builder().turnOnAdvancedStats();

        runInServer(builder, nc -> {
            AtomicInteger requests = new AtomicInteger();
            MessageHandler handler = (msg) -> { requests.incrementAndGet(); nc.publish(msg.getReplyTo(), null); };
            Dispatcher d1 = nc.createDispatcher(handler);
            Dispatcher d2 = nc.createDispatcher(handler);
            Dispatcher d3 = nc.createDispatcher(handler);
            Dispatcher d4 = nc.createDispatcher(msg -> { sleep(5000); handler.onMessage(msg); });
            d1.subscribe(SUBJECT);
            d2.subscribe(SUBJECT);
            d3.subscribe(SUBJECT);
            d4.subscribe(SUBJECT);

            Message reply = nc.request(SUBJECT, null, Duration.ofSeconds(2));
            assertNotNull(reply);
            sleep(2000);
            assertEquals(3, requests.get());
            NatsStatistics stats = (NatsStatistics)nc.getStatistics();
            assertEquals(1, stats.getRepliesReceived());
            assertEquals(2, stats.getDuplicateRepliesReceived());
            assertEquals(0, stats.getOrphanRepliesReceived());

            sleep(3100);
            assertEquals(4, requests.get());
            stats = (NatsStatistics)nc.getStatistics();
            assertEquals(1, stats.getRepliesReceived());
            assertEquals(2, stats.getDuplicateRepliesReceived());
            assertEquals(1, stats.getOrphanRepliesReceived());
        });
    }

    @Test
    public void testManualRequestReply() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
             Connection nc = Nats.connect(ts.getURI())) {
            assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");

            Dispatcher d = nc.createDispatcher((msg) -> nc.publish(msg.getReplyTo(), msg.getData()));
            d.subscribe("request");

            Subscription sub = nc.subscribe("reply");

            nc.publish("request", "reply", "hello".getBytes(StandardCharsets.UTF_8));

            Message msg = sub.nextMessage(Duration.ofMillis(400));

            assertNotNull(msg);
            assertEquals("hello", new String(msg.getData(), StandardCharsets.UTF_8));
        }
    }

    @Test
    public void testRequestWithCustomInboxPrefix() throws IOException, ExecutionException, TimeoutException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(new Options.Builder().inboxPrefix("myinbox").server(ts.getURI()).maxReconnects(0).build())) {
            assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
            
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
    public void testRequireCleanupOnTimeoutNoNoResponders() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server(ts.getURI())
                    .requestCleanupInterval(Duration.ofHours(1))
                    .noNoResponders().build();

            Connection nc = Nats.connect(options);
            try {
                assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");

                assertThrows(TimeoutException.class,
                        () -> nc.request("subject", null).get(100, TimeUnit.MILLISECONDS));

                assertEquals(1, ((NatsStatistics)nc.getStatistics()).getOutstandingRequests());
            } finally {
                nc.close();
                assertEquals(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testRequireCleanupOnTimeoutCleanCompletable() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {

            long cleanupInterval = 100;

            Options options = new Options.Builder().server(ts.getURI())
                    .requestCleanupInterval(Duration.ofMillis(cleanupInterval))
                    .noNoResponders().build();

            NatsConnection nc = (NatsConnection) Nats.connect(options);
            try {
                assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                NatsMessage nm = NatsMessage.builder().subject(SUBJECT).data(dataBytes(2)).build();
                CompletableFuture<Message> future = nc.requestWithTimeout(nm, Duration.ofMillis(cleanupInterval));
                
                Thread.sleep(2 * cleanupInterval + Options.DEFAULT_CONNECTION_TIMEOUT.toMillis());

                assertTrue(future.isCompletedExceptionally());
                assertEquals(0, ((NatsStatistics)nc.getStatistics()).getOutstandingRequests());

            } finally {
                nc.close();
                assertEquals(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testSimpleRequestWithTimeout() throws IOException, ExecutionException, TimeoutException, InterruptedException {

        try (NatsTestServer ts = new NatsTestServer(false))
        {
            Options options = new Options.Builder().server(ts.getURI()).requestCleanupInterval(Duration.ofHours(1)).build();
            NatsConnection nc = (NatsConnection) Nats.connect(options);

            try {

                assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");

                Dispatcher d = nc.createDispatcher((msg) -> {
                    assertTrue(msg.getReplyTo().startsWith(Options.DEFAULT_INBOX_PREFIX));
                    nc.publish(msg.getReplyTo(), null);
                });
                d.subscribe(SUBJECT);

                NatsMessage nm = NatsMessage.builder().subject(SUBJECT).data(dataBytes(2)).build();
                CompletableFuture<Message> incoming = nc.requestWithTimeout("subject", null, Duration.ofMillis(100));

                Message msg = incoming.get(500, TimeUnit.MILLISECONDS);

                assertEquals(0, ((NatsStatistics)nc.getStatistics()).getOutstandingRequests());
                assertNotNull(msg);
                assertEquals(0, msg.getData().length);
                assertTrue(msg.getSubject().indexOf('.') < msg.getSubject().lastIndexOf('.'));

            }
            finally {
                nc.close();
                assertEquals(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testSimpleRequestWithTimeoutSlowProducer() throws IOException, ExecutionException, TimeoutException, InterruptedException {

        try (NatsTestServer ts = new NatsTestServer(false))
        {
            long cleanupInterval = 10;
            Options options = new Options.Builder().server(ts.getURI()).requestCleanupInterval(Duration.ofMillis(cleanupInterval)).build();
            NatsConnection nc = (NatsConnection) Nats.connect(options);

            try {

                assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");

                //slow responder
                long delay = 2 * cleanupInterval + Options.DEFAULT_CONNECTION_TIMEOUT.toMillis();

                Dispatcher d = nc.createDispatcher((msg) -> {
                    assertTrue(msg.getReplyTo().startsWith(Options.DEFAULT_INBOX_PREFIX));
                    Thread.sleep(delay);
                    nc.publish(msg.getReplyTo(), null);
                });
                d.subscribe(SUBJECT);

                NatsMessage nm = NatsMessage.builder().subject(SUBJECT).data(dataBytes(2)).build();
                CompletableFuture<Message> incoming = nc.requestWithTimeout("subject", null, Duration.ofMillis(cleanupInterval));
                assertThrows(CancellationException.class, () -> incoming.get(delay, TimeUnit.MILLISECONDS));

            }
            finally {
                nc.close();
                assertEquals(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testRequireCleanupOnCancelFromNoResponders() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server(ts.getURI())
                    .requestCleanupInterval(Duration.ofHours(1)).build();

            Connection nc = Nats.connect(options);
            try {
                assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                assertThrows(CancellationException.class, () -> nc.request("subject", null).get(100, TimeUnit.MILLISECONDS));

                assertEquals(0, ((NatsStatistics) nc.getStatistics()).getOutstandingRequests());
            } finally {
                nc.close();
                assertEquals(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
            }

        }
    }

    @Test
    public void testRequireCleanupWithTimeoutNoResponders() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server(ts.getURI())
                    .requestCleanupInterval(Duration.ofHours(1)).build();

            Connection nc = Nats.connect(options);
            try {
                assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                assertThrows(CancellationException.class, () -> nc.requestWithTimeout("subject", null, Duration.ofMillis(100)).get(100, TimeUnit.MILLISECONDS));
                assertEquals(0, ((NatsStatistics) nc.getStatistics()).getOutstandingRequests());
            } finally {
                nc.close();
                assertEquals(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
            }

        }
    }

    @Test
    public void testRequireCleanupWithTimeoutNoNoResponders() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server(ts.getURI())
                    .requestCleanupInterval(Duration.ofHours(1))
                    .noNoResponders().build();

            Connection nc = Nats.connect(options);
            try {
                assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");

                assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                assertThrows(TimeoutException.class, () -> nc.requestWithTimeout("subject", null, Duration.ofMillis(100)).get(100, TimeUnit.MILLISECONDS));
                assertEquals(1, ((NatsStatistics)nc.getStatistics()).getOutstandingRequests());

            } finally {
                nc.close();
                assertEquals(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testRequireCleanupOnCancel() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server(ts.getURI()).requestCleanupInterval(Duration.ofHours(1)).build();
            Connection nc = Nats.connect(options);
            try {
                assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                
                Future<Message> incoming = nc.request("subject", null);
                incoming.cancel(true);

                assertEquals(1, ((NatsStatistics)nc.getStatistics()).getOutstandingRequests());
            } finally {
                nc.close();
                assertEquals(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testCleanupTimerWorks() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            long cleanupInterval = 50;
            Options options = new Options.Builder().server(ts.getURI()).requestCleanupInterval(Duration.ofMillis(cleanupInterval)).build();
            Connection nc = Nats.connect(options);
            try {
                assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                
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
                assertEquals(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
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
                assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                
                Dispatcher d = nc.createDispatcher((msg) -> nc.publish(msg.getReplyTo(), null));
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
                assertEquals(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
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
                    assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                    
                    Dispatcher d = nc.createDispatcher((msg) -> nc.publish(msg.getReplyTo(), new byte[1]));
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
                    assertEquals(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
                }
            }
    }

    @Test
    public void testOldStyleRequest() throws IOException, ExecutionException, TimeoutException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server(ts.getURI()).oldRequestStyle().build();
            Connection nc = Nats.connect(options);
            try {
                assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                
                Dispatcher d = nc.createDispatcher((msg) -> nc.publish(msg.getReplyTo(), null));
                d.subscribe("subject");

                Future<Message> incoming = nc.request("subject", null);
                Message msg = incoming.get(500, TimeUnit.MILLISECONDS);

                assertEquals(0, ((NatsStatistics)nc.getStatistics()).getOutstandingRequests());
                assertNotNull(msg);
                assertEquals(0, msg.getData().length);
                assertEquals(msg.getSubject().indexOf('.'), msg.getSubject().lastIndexOf('.'));
            } finally {
                nc.close();
                assertEquals(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
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
                assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                
                Dispatcher d = nc.createDispatcher((msg) -> nc.publish(msg.getReplyTo(), msg.getData()));
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
                assertEquals(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
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
                fail();
            }
        });
    }

    @Test
    public void testThrowsWithoutSubject() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
                nc.request((String)null, null);
                fail();
            }
        });
    }

    @Test
    public void testThrowsEmptySubject() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
                nc.request("", null);
                fail();
            }
        });
    }

    @Test
    public void testThrowsIfTooBig() {
        assertThrows(IllegalArgumentException.class, () -> {
            String customInfo = "{\"server_id\":\"myid\",\"max_payload\":512}";

            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(null, customInfo);
                    Connection nc = Nats.connect(ts.getURI())) {
                assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                assertEquals("myid", ((NatsConnection) nc).getInfo().getServerId(), "got custom info");
                assertEquals(512, ((NatsConnection) nc).getInfo().getMaxPayload(), "got custom info");

                byte[] body = new byte[513];
                nc.request("subject", body);
                fail();
            }
        });
    }

    @Test
    public void testNatsRequestCompletableFuture() {
        NatsRequestCompletableFuture f = new NatsRequestCompletableFuture(true, Duration.ofHours(-1));
        assertTrue(f.hasExceededTimeout());
        assertFalse(f.wasCancelledClosing());
        assertFalse(f.wasCancelledTimedOut());
        f.cancelClosing(); // not real use, just testing flags
        f.cancelTimedOut(); // not real use, just testing flags
        assertTrue(f.wasCancelledClosing());
        assertTrue(f.wasCancelledTimedOut());
    }

    @Test
    public void testNatsImplAndEmptyStatsCoverage() {
        Statistics s = NatsImpl.createEmptyStats();
        assertEquals(0, s.getInBytes());
        assertEquals(0, s.getInMsgs());
        assertEquals(0, s.getOutBytes());
        assertEquals(0, s.getOutMsgs());
        assertEquals(0, s.getDroppedCount());
        assertEquals(0, s.getReconnects());
    }
}
