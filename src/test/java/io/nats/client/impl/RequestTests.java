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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.support.NatsRequestCompletableFuture.CancelAction;
import static io.nats.client.utils.ConnectionUtils.standardConnectionWait;
import static io.nats.client.utils.OptionsUtils.optionsBuilder;
import static io.nats.client.utils.ThreadUtils.sleep;
import static org.junit.jupiter.api.Assertions.*;

public class RequestTests extends TestBase {
    @Test
    public void testSimpleRequest() throws Exception {
        runInSharedOwnNc(optionsBuilder().maxReconnects(0), nc -> {
            assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");

            AtomicReference<String> replyTo = new AtomicReference<>();
            Dispatcher d = nc.createDispatcher(msg -> {
                replyTo.set(msg.getReplyTo());
                assertTrue(msg.getReplyTo().startsWith(Options.DEFAULT_INBOX_PREFIX));
                if (msg.hasHeaders()) {
                    nc.publish(msg.getReplyTo(), msg.getHeaders(), null);
                }
                else {
                    nc.publish(msg.getReplyTo(), null);
                }
            });
            d.subscribe("subject");

            Future<Message> incoming = nc.request("subject", null);
            Message msg = incoming.get(500, TimeUnit.MILLISECONDS);

            assertEquals(0, nc.getStatistics().getOutstandingRequests());
            assertNotNull(msg);
            assertEquals(0, msg.getData().length);
            assertTrue(msg.getSubject().indexOf('.') < msg.getSubject().lastIndexOf('.'));

            incoming = nc.request("subject", new Headers().put("foo", "bar"), null);
            msg = incoming.get(500, TimeUnit.MILLISECONDS);

            assertEquals(0, nc.getStatistics().getOutstandingRequests());
            assertNotNull(msg);
            assertEquals(0, msg.getData().length);
            assertEquals(msg.getSubject(), replyTo.get());
            assertTrue(msg.hasHeaders());
            assertEquals("bar", msg.getHeaders().getFirst("foo"));
        });
    }

    @Test
    public void testRequestVarieties() throws Exception {
        runInSharedOwnNc(nc -> {
            Dispatcher d = nc.createDispatcher(msg -> {
                if (msg.hasHeaders()) {
                    nc.publish(msg.getReplyTo(), msg.getHeaders(), msg.getData());
                }
                else {
                    nc.publish(msg.getReplyTo(), msg.getData());
                }
            });
            String subject = random();
            d.subscribe(subject);

            Future<Message> f = nc.request(subject, dataBytes(1));
            Message msg = f.get(500, TimeUnit.MILLISECONDS);
            assertEquals(data(1), new String(msg.getData()));

            NatsMessage outMsg = NatsMessage.builder().subject(subject).data(dataBytes(2)).build();
            f = nc.request(outMsg);
            msg = f.get(500, TimeUnit.MILLISECONDS);
            assertNotNull(msg);
            assertNotNull(msg.getData());
            assertEquals(data(2), new String(msg.getData()));

            msg = nc.request(subject, dataBytes(3), Duration.ofSeconds(1));
            assertNotNull(msg);
            assertNotNull(msg.getData());
            assertEquals(data(3), new String(msg.getData()));

            outMsg = NatsMessage.builder().subject(subject).data(dataBytes(4)).build();
            msg = nc.request(outMsg, Duration.ofSeconds(1));
            assertNotNull(msg);
            assertNotNull(msg.getData());
            assertEquals(data(4), new String(msg.getData()));

            msg = nc.request(subject, new Headers().put("foo", "bar"), dataBytes(5), Duration.ofSeconds(1));
            assertNotNull(msg);
            assertNotNull(msg.getData());
            assertEquals(data(5), new String(msg.getData()));
            assertTrue(msg.hasHeaders());
            assertEquals("bar", msg.getHeaders().getFirst("foo"));

            //noinspection DataFlowIssue
            assertThrows(IllegalArgumentException.class, () -> nc.request(null));
            //noinspection DataFlowIssue
            assertThrows(IllegalArgumentException.class, () -> nc.request(null, Duration.ofSeconds(1)));
        });
    }

    @Test
    public void testSimpleResponseMessageHasConnection() throws Exception {
        try (NatsTestServer ts = new NatsTestServer();
                Connection nc = Nats.connect(optionsBuilder(ts).maxReconnects(0).build())) {
            assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
            
            Dispatcher d = nc.createDispatcher(msg -> {
                assertTrue(msg.getReplyTo().startsWith(Options.DEFAULT_INBOX_PREFIX));
                msg.getConnection().publish(msg.getReplyTo(), null);
            });
            d.subscribe("subject");

            Future<Message> incoming = nc.request("subject", null);
            Message msg = incoming.get(5000, TimeUnit.MILLISECONDS);

            assertEquals(0, nc.getStatistics().getOutstandingRequests());
            assertNotNull(msg);
            assertEquals(0, msg.getData().length);
            assertTrue(msg.getSubject().indexOf('.') < msg.getSubject().lastIndexOf('.'));
            assertEquals(msg.getConnection(), nc);
        }
    }

    @Test
    public void testSafeRequest() throws Exception {
        try (NatsTestServer ts = new NatsTestServer();
                Connection nc = Nats.connect(optionsBuilder(ts).maxReconnects(0).build())) {
            assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
            
            Dispatcher d = nc.createDispatcher(msg -> nc.publish(msg.getReplyTo(), null));
            d.subscribe("subject");

            Message msg = nc.request("subject", null, Duration.ofMillis(1000));

            assertEquals(0, nc.getStatistics().getOutstandingRequests());
            assertNotNull(msg);
            assertEquals(0, msg.getData().length);
            assertTrue(msg.getSubject().indexOf('.') < msg.getSubject().lastIndexOf('.'));
        }
    }

    @Test
    public void testMultipleRequest() throws Exception {
        try (NatsTestServer ts = new NatsTestServer();
                Connection nc = Nats.connect(optionsBuilder(ts).maxReconnects(0).build())) {
            assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
            
            Dispatcher d = nc.createDispatcher(msg -> nc.publish(msg.getReplyTo(), new byte[7]));
            d.subscribe("subject");

            for (int i=0; i<10; i++) {
                Future<Message> incoming = nc.request("subject", new byte[11]);
                Message msg = incoming.get(500, TimeUnit.MILLISECONDS);

                assertEquals(0, nc.getStatistics().getOutstandingRequests());
                assertNotNull(msg);
                assertEquals(7, msg.getData().length);
                assertTrue(msg.getSubject().indexOf('.') < msg.getSubject().lastIndexOf('.'));
            }
        }
    }

    @Test
    public void testMultipleReplies() throws Exception {
        Options.Builder builder = optionsBuilder().turnOnAdvancedStats().requestCleanupInterval(Duration.ofMillis(2500));
        runInSharedOwnNc(builder, nc -> {
            CountDownLatch d4CanReply = new CountDownLatch(1);
            AtomicInteger requests = new AtomicInteger();
            MessageHandler handler = msg -> { requests.incrementAndGet(); nc.publish(msg.getReplyTo(), null); };
            Dispatcher d1 = nc.createDispatcher(handler);
            Dispatcher d2 = nc.createDispatcher(handler);
            Dispatcher d3 = nc.createDispatcher(handler);
            Dispatcher d4 = nc.createDispatcher(msg -> {
                requests.incrementAndGet();
                d4CanReply.await(5, TimeUnit.SECONDS);
                nc.publish(msg.getReplyTo(), null);
            });

            String subject = random();
            d1.subscribe(subject);
            d2.subscribe(subject);
            d3.subscribe(subject);
            d4.subscribe(subject);

            Message reply = nc.request(subject, null, Duration.ofSeconds(2));
            assertNotNull(reply);
            sleep(2000); // less than the requestCleanupInterval but enough time
            assertEquals(4, requests.get());
            NatsStatistics stats = (NatsStatistics)nc.getStatistics();
            assertEquals(1, stats.getRepliesReceived());
            assertEquals(2, stats.getDuplicateRepliesReceived());
            assertEquals(0, stats.getOrphanRepliesReceived());

            sleep(1000);            // easily over the requestCleanupInterval
            d4CanReply.countDown(); // signals d4 to reply

            sleep(1000);            // enough time for d4's reply to get processed
            stats = (NatsStatistics)nc.getStatistics();
            assertEquals(1, stats.getRepliesReceived());
            assertEquals(2, stats.getDuplicateRepliesReceived());
            assertEquals(1, stats.getOrphanRepliesReceived());
        });
    }

    @Test
    public void testManualRequestReplyAndPublishSignatures() throws Exception {
        try (NatsTestServer ts = new NatsTestServer();
             Connection nc = Nats.connect(ts.getLocalhostUri())) {
            assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");

            Dispatcher d = nc.createDispatcher(msg -> {
                // also getting coverage here of multiple publish signatures
                if (msg.hasHeaders()) {
                    nc.publish(msg.getReplyTo(), msg.getHeaders(), msg.getData());
                }
                else {
                    nc.publish(msg.getReplyTo(), msg.getData());
                }
            });
            d.subscribe("request-subject");

            Subscription sub = nc.subscribe("reply-to");

            nc.publish("request-subject", "reply-to", "hello".getBytes(StandardCharsets.UTF_8));

            Message msg = sub.nextMessage(Duration.ofMillis(400));

            assertNotNull(msg);
            assertEquals("hello", new String(msg.getData(), StandardCharsets.UTF_8));

            nc.publish("request-subject", "reply-to", new Headers().put("foo", "bar"), "check-headers".getBytes(StandardCharsets.UTF_8));

            msg = sub.nextMessage(Duration.ofMillis(400));

            assertEquals("check-headers", new String(msg.getData(), StandardCharsets.UTF_8));
            assertTrue(msg.hasHeaders());
            assertEquals("bar", msg.getHeaders().getFirst("foo"));
        }
    }

    @Test
    public void testRequestWithCustomInboxPrefix() throws Exception {
        try (NatsTestServer ts = new NatsTestServer();
                Connection nc = Nats.connect(optionsBuilder(ts).inboxPrefix("myinbox").maxReconnects(0).build())) {
            assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
            
            Dispatcher d = nc.createDispatcher(msg -> {
                assertTrue(msg.getReplyTo().startsWith("myinbox"));
                nc.publish(msg.getReplyTo(), null);
            });
            d.subscribe("subject");

            Future<Message> incoming = nc.request("subject", null);
            Message msg = incoming.get(500, TimeUnit.MILLISECONDS);

            assertEquals(0, nc.getStatistics().getOutstandingRequests());
            assertNotNull(msg);
            assertEquals(0, msg.getData().length);
            assertTrue(msg.getSubject().indexOf('.') < msg.getSubject().lastIndexOf('.'));
        }
    }

    @Test
    public void testRequireCleanupOnTimeoutNoNoResponders() throws Exception {
        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = optionsBuilder(ts)
                    .requestCleanupInterval(Duration.ofHours(1))
                    .noNoResponders().build();

            Connection nc = Nats.connect(options);
            try {
                assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");

                assertThrows(TimeoutException.class,
                        () -> nc.request("subject", null).get(100, TimeUnit.MILLISECONDS));

                assertEquals(1, nc.getStatistics().getOutstandingRequests());
            } finally {
                nc.close();
                assertEquals(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testRequireCleanupOnTimeoutCleanCompletable() throws Exception {
        try (NatsTestServer ts = new NatsTestServer()) {

            long cleanupInterval = 100;

            Options options = optionsBuilder(ts)
                    .requestCleanupInterval(Duration.ofMillis(cleanupInterval))
                    .noNoResponders().build();

            NatsConnection nc = (NatsConnection) Nats.connect(options);
            try {
                assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                NatsMessage nm = NatsMessage.builder().subject(random()).data(dataBytes(2)).build();
                CompletableFuture<Message> future = nc.requestWithTimeout(nm, Duration.ofMillis(cleanupInterval));
                
                Thread.sleep(2 * cleanupInterval + Options.DEFAULT_CONNECTION_TIMEOUT.toMillis());

                assertTrue(future.isCompletedExceptionally());
                assertEquals(0, nc.getStatistics().getOutstandingRequests());

            } finally {
                nc.close();
                assertEquals(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testSimpleRequestWithTimeout() throws Exception {

        try (NatsTestServer ts = new NatsTestServer())
        {
            Options options = optionsBuilder(ts).requestCleanupInterval(Duration.ofHours(1)).build();
            Connection nc = Nats.connect(options);

            try {

                assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");

                Dispatcher d = nc.createDispatcher(msg -> {
                    assertTrue(msg.getReplyTo().startsWith(Options.DEFAULT_INBOX_PREFIX));
                    if (msg.hasHeaders()) {
                        nc.publish(msg.getReplyTo(), msg.getHeaders(), null);
                    }
                    else {
                        nc.publish(msg.getReplyTo(), null);
                    }
                });
                String subject = random();
                d.subscribe(subject);

                CompletableFuture<Message> incoming = nc.requestWithTimeout(subject, null, Duration.ofMillis(100));

                Message msg = incoming.get(500, TimeUnit.MILLISECONDS);

                assertEquals(0, nc.getStatistics().getOutstandingRequests());
                assertNotNull(msg);
                assertEquals(0, msg.getData().length);
                assertTrue(msg.getSubject().indexOf('.') < msg.getSubject().lastIndexOf('.'));

                incoming = nc.requestWithTimeout(subject, new Headers().put("foo", "bar"), null, Duration.ofMillis(100));

                msg = incoming.get(500, TimeUnit.MILLISECONDS);

                assertEquals(0, nc.getStatistics().getOutstandingRequests());
                assertNotNull(msg);
                assertEquals(0, msg.getData().length);
                assertTrue(msg.getSubject().indexOf('.') < msg.getSubject().lastIndexOf('.'));
                assertTrue(msg.hasHeaders());
                assertEquals("bar", msg.getHeaders().getFirst("foo"));
            }
            finally {
                nc.close();
                assertEquals(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testSimpleRequestWithTimeoutSlowProducer() throws Exception {
        try (NatsTestServer ts = new NatsTestServer()) {
            long cleanupInterval = 10;
            Options options = optionsBuilder(ts).requestCleanupInterval(Duration.ofMillis(cleanupInterval)).build();
            NatsConnection nc = (NatsConnection) Nats.connect(options);

            try {

                assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");

                //slow responder
                long delay = 2 * cleanupInterval + Options.DEFAULT_CONNECTION_TIMEOUT.toMillis();

                Dispatcher d = nc.createDispatcher(msg -> {
                    assertTrue(msg.getReplyTo().startsWith(Options.DEFAULT_INBOX_PREFIX));
                    Thread.sleep(delay);
                    nc.publish(msg.getReplyTo(), null);
                });
                String subject = random();
                d.subscribe(subject);

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
    public void testRequireCleanupOnCancelFromNoResponders() throws Exception {
        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = optionsBuilder(ts)
                    .requestCleanupInterval(Duration.ofHours(1)).build();

            Connection nc = Nats.connect(options);
            try {
                assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                assertThrows(CancellationException.class, () -> nc.request("subject", null).get(100, TimeUnit.MILLISECONDS));

                assertEquals(0, nc.getStatistics().getOutstandingRequests());
            } finally {
                nc.close();
                assertEquals(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
            }

        }
    }

    @Test
    public void testRequireCleanupWithTimeoutNoResponders() throws Exception {
        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = optionsBuilder(ts)
                    .requestCleanupInterval(Duration.ofHours(1)).build();

            Connection nc = Nats.connect(options);
            try {
                assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                assertThrows(CancellationException.class, () -> nc.requestWithTimeout("subject", null, Duration.ofMillis(100)).get(100, TimeUnit.MILLISECONDS));
                assertEquals(0, nc.getStatistics().getOutstandingRequests());
            } finally {
                nc.close();
                assertEquals(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
            }

        }
    }

    @Test
    public void testRequireCleanupWithTimeoutNoNoResponders() throws Exception {
        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = optionsBuilder(ts)
                    .requestCleanupInterval(Duration.ofHours(1))
                    .noNoResponders().build();

            Connection nc = Nats.connect(options);
            try {
                assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");

                assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                assertThrows(TimeoutException.class, () -> nc.requestWithTimeout("subject", null, Duration.ofMillis(100)).get(100, TimeUnit.MILLISECONDS));
                assertEquals(1, nc.getStatistics().getOutstandingRequests());

            } finally {
                nc.close();
                assertEquals(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testRequireCleanupOnCancel() throws Exception {
        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = optionsBuilder(ts).requestCleanupInterval(Duration.ofHours(1)).build();
            Connection nc = Nats.connect(options);
            try {
                assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                NatsRequestCompletableFuture incoming = (NatsRequestCompletableFuture)nc.request("subject", null);
                incoming.cancel(true);
                NatsStatistics stats = (NatsStatistics)nc.getStatistics();
                // sometimes if the machine is very fast, the request gets a reply (even if it's no responders)
                // so there is either an outstanding or a received
                assertEquals(1, stats.getOutstandingRequests() + stats.getRepliesReceived());
            }
            finally {
                nc.close();
                assertEquals(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testCleanupTimerWorks() throws Exception {
        try (NatsTestServer ts = new NatsTestServer()) {
            long cleanupInterval = 50;
            Options options = optionsBuilder(ts).requestCleanupInterval(Duration.ofMillis(cleanupInterval)).build();
            Connection nc = Nats.connect(options);
            try {
                assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                
                Future<Message> incoming = nc.request("subject", null);
                incoming.cancel(true);
                incoming = nc.request("subject", null);
                incoming.cancel(true);
                incoming = nc.request("subject", null);
                incoming.cancel(true);

                long sleep = 2 * cleanupInterval;
                long timeout = 10 * cleanupInterval;

                sleep(sleep);
                assertTrueByTimeout(timeout, () -> nc.getStatistics().getOutstandingRequests() == 0);

                // Make sure it is still running
                incoming = nc.request("subject", null);
                incoming.cancel(true);
                incoming = nc.request("subject", null);
                incoming.cancel(true);
                incoming = nc.request("subject", null);
                incoming.cancel(true);

                sleep(sleep);
                assertTrueByTimeout(timeout, () -> nc.getStatistics().getOutstandingRequests() == 0);
            } finally {
                nc.close();
                assertEquals(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testRequestsVsCleanup() throws Exception {
        try (NatsTestServer ts = new NatsTestServer()) {
            long cleanupInterval = 50;
            int msgCount = 100;
            Options options = optionsBuilder(ts).requestCleanupInterval(Duration.ofMillis(cleanupInterval)).build();
            Connection nc = Nats.connect(options);
            try {
                assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                
                Dispatcher d = nc.createDispatcher(msg -> nc.publish(msg.getReplyTo(), null));
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
                assertTrue(0 >= nc.getStatistics().getOutstandingRequests());
            } finally {
                nc.close();
                assertEquals(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
            }
        }
    }

        @Test
        public void testDelayInPickingUpFuture() throws Exception {
            try (NatsTestServer ts = new NatsTestServer()) {
                int msgCount = 100;
                ArrayList<Future<Message>> messages = new ArrayList<>();
                Connection nc = Nats.connect(ts.getLocalhostUri());
                try {
                    assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                    
                    Dispatcher d = nc.createDispatcher(msg -> nc.publish(msg.getReplyTo(), new byte[1]));
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
        
                    assertEquals(0, nc.getStatistics().getOutstandingRequests());
                } finally {
                    nc.close();
                    assertEquals(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
                }
            }
    }

    @Test
    public void testOldStyleRequest() throws Exception {
        runInSharedOwnNc(Options.builder().oldRequestStyle(), nc -> {
            String subject = random();
            AtomicReference<String> replyTo = new AtomicReference<>();
            Dispatcher d = nc.createDispatcher(msg -> {
                replyTo.set(msg.getReplyTo());
                nc.publish(msg.getReplyTo(), null);
            });
            d.subscribe(subject);

            Future<Message> incoming = nc.request(subject, null);
            Message msg = incoming.get(500, TimeUnit.MILLISECONDS);

            assertEquals(0, nc.getStatistics().getOutstandingRequests());
            assertNotNull(msg);
            assertEquals(0, msg.getData().length);
            assertEquals(msg.getSubject(), replyTo.get());
        });
    }

    @Test
    public void testBuffersResize() throws Exception {
        try (NatsTestServer ts = new NatsTestServer()) {
            int initialSize = 128;
            int messageSize = 1024;
            Options options = optionsBuilder(ts).bufferSize(initialSize).connectionTimeout(Duration.ofSeconds(10)).build();
            try (Connection nc = standardConnectionWait(options)) {
                Dispatcher d = nc.createDispatcher(msg -> nc.publish(msg.getReplyTo(), msg.getData()));
                d.subscribe("subject");

                Future<Message> incoming = nc.request("subject", new byte[messageSize]); // force the buffers to resize
                Message msg = null;

                try {
                    msg = incoming.get(30000, TimeUnit.MILLISECONDS);
                } catch (Exception exp) {
                    exp.printStackTrace();
                }

                assertEquals(0, nc.getStatistics().getOutstandingRequests());
                assertNotNull(msg);
                assertEquals(messageSize, msg.getData().length);
            }
        }
    }
    
    @Test
    public void testRequestErrors() throws Exception {
        runInSharedOwnNc(nc -> {
            //noinspection DataFlowIssue
            assertThrows(IllegalArgumentException.class, () -> nc.request((String)null, null)); // null subject bad
            assertThrows(IllegalArgumentException.class, () -> nc.request("", null)); // empty subject bad
            nc.close();
            assertThrows(IllegalStateException.class, () -> nc.request("subject", null)); // can't request after close
        });
    }

    @Test
    public void testNatsRequestCompletableFuture() throws Exception {
        // coverage for configuration
        NatsRequestCompletableFuture f = new NatsRequestCompletableFuture(CancelAction.CANCEL, Duration.ofMillis(-1000), true);
        assertEquals(CancelAction.CANCEL, f.getCancelAction());
        assertTrue(f.hasExceededTimeout());
        assertFalse(f.wasCancelledClosing());
        assertFalse(f.wasCancelledTimedOut());
        f.cancelClosing(); // not real use, just testing flags
        f.cancelTimedOut(); // not real use, just testing flags
        assertTrue(f.wasCancelledClosing());
        assertTrue(f.wasCancelledTimedOut());
        assertTrue(f.useTimeoutException());

        f = new NatsRequestCompletableFuture(CancelAction.COMPLETE, Duration.ofNanos(0), true);
        assertEquals(CancelAction.COMPLETE, f.getCancelAction());

        f = new NatsRequestCompletableFuture(CancelAction.REPORT, Duration.ofNanos(0), true);
        assertEquals(CancelAction.REPORT, f.getCancelAction());

        // coverage for null timeout
        f = new NatsRequestCompletableFuture(CancelAction.CANCEL, null, true);
        Thread.sleep(Options.DEFAULT_REQUEST_CLEANUP_INTERVAL.toMillis() + 100);
        assertTrue(f.hasExceededTimeout());

        f = new NatsRequestCompletableFuture(CancelAction.CANCEL, Duration.ofNanos(0), false);
        assertFalse(f.useTimeoutException());

        // coverage for behavior
        NatsRequestCompletableFuture fcf = new NatsRequestCompletableFuture(CancelAction.CANCEL, Duration.ofNanos(0), false);
        assertFalse(fcf.useTimeoutException());
        fcf.cancelClosing();
        assertThrows(CancellationException.class, fcf::get);

        NatsRequestCompletableFuture fct = new NatsRequestCompletableFuture(CancelAction.CANCEL, Duration.ofNanos(0), true);
        assertTrue(fct.useTimeoutException());
        fct.cancelClosing();
        assertThrows(CancellationException.class, fct::get);

        NatsRequestCompletableFuture ftof = new NatsRequestCompletableFuture(CancelAction.CANCEL, Duration.ofNanos(0), false);
        assertFalse(ftof.useTimeoutException());
        ftof.cancelTimedOut();
        assertThrows(CancellationException.class, ftof::get);

        NatsRequestCompletableFuture ftot = new NatsRequestCompletableFuture(CancelAction.CANCEL, Duration.ofNanos(0), true);
        assertTrue(ftot.useTimeoutException());
        ftot.cancelTimedOut();
        ExecutionException ee = assertThrows(ExecutionException.class, ftot::get);
        assertInstanceOf(TimeoutException.class, ee.getCause());
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

    @Test
    public void testCancelledFutureMustNotErrorOnCleanResponses() throws Exception {
        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = Options.builder()
                    .server(ts.getLocalhostUri())
                    .noNoResponders()
                    .requestCleanupInterval(Duration.ofSeconds(10))
                    .build();
            NatsConnection nc = (NatsConnection) Nats.connect(options);

            NatsRequestCompletableFuture future = (NatsRequestCompletableFuture) nc.request("request", null);
            future.cancelClosing();

            // Future is already cancelled, collecting it shouldn't result in an exception being thrown.
            assertDoesNotThrow(() -> nc.cleanResponses(false));
        }
    }

    @Test
    public void testRtt() throws Exception {
        runInShared(nc -> {
            assertTrue(nc.RTT().toMillis() < 50);
        });
    }
}
