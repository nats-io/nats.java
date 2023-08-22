// Copyright 2023 The NATS Authors
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
import io.nats.client.api.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.nats.client.BaseConsumeOptions.*;
import static io.nats.client.impl.JetStreamConsumerTests.EXPECTED_CON_SEQ_NUMS;
import static org.junit.jupiter.api.Assertions.*;

public class SimplificationTests extends JetStreamTestBase {

    private boolean mustBeAtLeast291(ServerInfo si) {
        return si.isSameOrNewerThanVersion("2.9.1");
    }

    @Test
    public void testStreamContext() throws Exception {
        runInJsServer(this::mustBeAtLeast291, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            assertThrows(JetStreamApiException.class, () -> nc.getStreamContext(STREAM));
            assertThrows(JetStreamApiException.class, () -> nc.getStreamContext(STREAM, JetStreamOptions.DEFAULT_JS_OPTIONS));
            assertThrows(JetStreamApiException.class, () -> js.getStreamContext(STREAM));

            CreateStreamResult csr = createMemoryStream(jsm);
            StreamContext streamContext = nc.getStreamContext(csr.stream);
            assertEquals(csr.stream, streamContext.getStreamName());
            _testStreamContext(js, csr, streamContext);

            csr = createMemoryStream(jsm);
            streamContext = js.getStreamContext(csr.stream);
            assertEquals(csr.stream, streamContext.getStreamName());
            _testStreamContext(js, csr, streamContext);
        });
    }

    private static void _testStreamContext(JetStream js, CreateStreamResult csr, StreamContext streamContext) throws IOException, JetStreamApiException {
        String durable = durable();
        assertThrows(JetStreamApiException.class, () -> streamContext.getConsumerContext(durable));
        assertThrows(JetStreamApiException.class, () -> streamContext.deleteConsumer(durable));

        ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(durable).build();
        ConsumerContext consumerContext = streamContext.createOrUpdateConsumer(cc);
        ConsumerInfo ci = consumerContext.getConsumerInfo();
        assertEquals(csr.stream, ci.getStreamName());
        assertEquals(durable, ci.getName());

        ci = streamContext.getConsumerInfo(durable);
        assertNotNull(ci);
        assertEquals(csr.stream, ci.getStreamName());
        assertEquals(durable, ci.getName());

        assertEquals(1, streamContext.getConsumerNames().size());

        assertEquals(1, streamContext.getConsumers().size());
        consumerContext = streamContext.getConsumerContext(durable);
        assertNotNull(consumerContext);
        assertEquals(durable, consumerContext.getConsumerName());

        ci = consumerContext.getConsumerInfo();
        assertNotNull(ci);
        assertEquals(csr.stream, ci.getStreamName());
        assertEquals(durable, ci.getName());

        ci = consumerContext.getCachedConsumerInfo();
        assertNotNull(ci);
        assertEquals(csr.stream, ci.getStreamName());
        assertEquals(durable, ci.getName());

        streamContext.deleteConsumer(durable);

        assertThrows(JetStreamApiException.class, () -> streamContext.getConsumerContext(durable));
        assertThrows(JetStreamApiException.class, () -> streamContext.deleteConsumer(durable));

        // coverage
        js.publish(csr.subject, "one".getBytes());
        js.publish(csr.subject, "two".getBytes());
        js.publish(csr.subject, "three".getBytes());
        js.publish(csr.subject, "four".getBytes());
        js.publish(csr.subject, "five".getBytes());
        js.publish(csr.subject, "six".getBytes());

        assertTrue(streamContext.deleteMessage(3));
        assertTrue(streamContext.deleteMessage(4, true));

        MessageInfo mi = streamContext.getMessage(1);
        assertEquals(1, mi.getSeq());

        mi = streamContext.getFirstMessage(csr.subject);
        assertEquals(1, mi.getSeq());

        mi = streamContext.getLastMessage(csr.subject);
        assertEquals(6, mi.getSeq());

        mi = streamContext.getNextMessage(3, csr.subject);
        assertEquals(5, mi.getSeq());

        assertNotNull(streamContext.getStreamInfo());
        assertNotNull(streamContext.getStreamInfo(StreamInfoOptions.builder().build()));

        streamContext.purge(PurgeOptions.builder().sequence(5).build());
        assertThrows(JetStreamApiException.class, () -> streamContext.getMessage(1));

        mi = streamContext.getFirstMessage(csr.subject);
        assertEquals(5, mi.getSeq());

        streamContext.purge();
        assertThrows(JetStreamApiException.class, () -> streamContext.getFirstMessage(csr.subject));
    }

    static int FETCH_EPHEMERAL = 1;
    static int FETCH_DURABLE = 2;
    static int FETCH_ORDERED = 3;
    @Test
    public void testFetch() throws Exception {
        runInJsServer(this::mustBeAtLeast291, nc -> {
            CreateStreamResult csr = createMemoryStream(nc);
            JetStream js = nc.jetStream();
            for (int x = 1; x <= 20; x++) {
                js.publish(csr.subject, ("test-fetch-msg-" + x).getBytes());
            }

            for (int f = FETCH_EPHEMERAL; f <= FETCH_ORDERED; f++) {
                // 1. Different fetch sizes demonstrate expiration behavior

                // 1A. equal number of messages than the fetch size
                _testFetch("1A", nc, csr, 20, 0, 20, f);

                // 1B. more messages than the fetch size
                _testFetch("1B", nc, csr, 10, 0, 10, f);

                // 1C. fewer messages than the fetch size
                _testFetch("1C", nc, csr, 40, 0, 40, f);

                // 1D. simple-consumer-40msgs was created in 1C and has no messages available
                _testFetch("1D", nc, csr, 40, 0, 40, f);

                // 2. Different max bytes sizes demonstrate expiration behavior
                //    - each test message is approximately 100 bytes

                // 2A. max bytes is reached before message count
                _testFetch("2A", nc, csr, 0, 750, 20, f);

                // 2B. fetch size is reached before byte count
                _testFetch("2B", nc, csr, 10, 1500, 10, f);

                if (f > FETCH_EPHEMERAL) {
                    // 2C. fewer bytes than the byte count
                    _testFetch("2C", nc, csr, 0, 3000, 40, f);
                }
            }
        });
    }

    private static void _testFetch(String label, Connection nc, CreateStreamResult csr, int maxMessages, int maxBytes, int testAmount, int fetchType) throws Exception {
        JetStreamManagement jsm = nc.jetStreamManagement();
        JetStream js = nc.jetStream();

        StreamContext sc = js.getStreamContext(csr.stream);

        BaseConsumerContext consumerContext;
        if (fetchType == FETCH_ORDERED) {
            consumerContext = sc.createOrderedConsumer(new OrderedConsumerConfiguration());
            // coverage
        }
        else {
            // Pre define a consumer
            String name = generateConsumerName(maxMessages, maxBytes);
            ConsumerConfiguration.Builder builder = ConsumerConfiguration.builder();
            ConsumerConfiguration cc;
            if (fetchType == FETCH_DURABLE) {
                name = name + "D";
                cc = builder.durable(name).build();
            }
            else {
                name = name + "E";
                cc = builder.name(name).inactiveThreshold(10_000).build();
            }
            jsm.addOrUpdateConsumer(csr.stream, cc);

            // Consumer[Context]
            consumerContext = sc.getConsumerContext(name);
        }

        // Custom consume options
        FetchConsumeOptions.Builder builder = FetchConsumeOptions.builder().expiresIn(2000);
        if (maxMessages == 0) {
            builder.maxBytes(maxBytes);
        }
        else if (maxBytes == 0) {
            builder.maxMessages(maxMessages);
        }
        else {
            builder.max(maxBytes, maxMessages);
        }
        FetchConsumeOptions fetchConsumeOptions = builder.build();

        long start = System.currentTimeMillis();

        int rcvd = 0;
        long elapsed;
        // create the consumer then use it
        try (FetchConsumer consumer = consumerContext.fetch(fetchConsumeOptions)) {
            Message msg = consumer.nextMessage();
            while (msg != null) {
                ++rcvd;
                msg.ack();
                msg = consumer.nextMessage();
            }
            elapsed = System.currentTimeMillis() - start;
        }

        switch (label) {
            case "1A":
            case "1B":
            case "2B":
                assertEquals(testAmount, rcvd);
                assertTrue(elapsed < 100);
                break;
            case "1C":
            case "1D":
            case "2C":
                assertTrue(rcvd < testAmount);
                assertTrue(elapsed >= 1500);
                break;
            case "2A":
                assertTrue(rcvd < testAmount);
                assertTrue(elapsed < 100);
                break;
        }
    }

    private static String generateConsumerName(int maxMessages, int maxBytes) {
        return maxBytes == 0
            ? NAME + "-" + maxMessages + "msgs"
            : NAME + "-" + maxBytes + "bytes-" + maxMessages + "msgs";
    }

    @Test
    public void testIterableConsumer() throws Exception {
        runInJsServer(this::mustBeAtLeast291, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            createDefaultTestStream(jsm);
            JetStream js = nc.jetStream();

            // Pre define a consumer
            ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(DURABLE).build();
            jsm.addOrUpdateConsumer(STREAM, cc);

            // Consumer[Context]
            ConsumerContext consumerContext = js.getConsumerContext(STREAM, DURABLE);

            int stopCount = 500;
            // create the consumer then use it
            try (IterableConsumer consumer = consumerContext.iterate()) {
                _testIterable(js, stopCount, consumer);
            }

            // coverage
            IterableConsumer consumer = consumerContext.iterate(ConsumeOptions.DEFAULT_CONSUME_OPTIONS);
            consumer.close();
            assertThrows(IllegalArgumentException.class, () -> consumerContext.iterate((ConsumeOptions) null));
        });
    }

    @Test
    public void testOrderedIterableConsumerBasic() throws Exception {
        runInJsServer(this::mustBeAtLeast291, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();
            createDefaultTestStream(jsm);

            StreamContext sc = nc.getStreamContext(STREAM);

            int stopCount = 500;
            OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration().filterSubject(SUBJECT);
            OrderedConsumerContext ctx = sc.createOrderedConsumer(occ);
            try (IterableConsumer consumer = ctx.iterate()) {
                _testIterable(js, stopCount, consumer);
            }
        });
    }

    private static void _testIterable(JetStream js, int stopCount, IterableConsumer consumer) throws InterruptedException {
        AtomicInteger count = new AtomicInteger();
        Thread consumeThread = new Thread(() -> {
            try {
                while (count.get() < stopCount) {
                    Message msg = consumer.nextMessage(1000);
                    if (msg != null) {
                        msg.ack();
                        count.incrementAndGet();
                    }
                }

                Thread.sleep(50); // allows more messages to come across
                consumer.stop();

                Message msg = consumer.nextMessage(1000);
                while (msg != null) {
                    msg.ack();
                    count.incrementAndGet();
                    msg = consumer.nextMessage(1000);
                }
            }
            catch (Exception e) {
                fail(e);
            }
        });
        consumeThread.start();

        Publisher publisher = new Publisher(js, SUBJECT, 25);
        Thread pubThread = new Thread(publisher);
        pubThread.start();

        consumeThread.join();
        publisher.stop();
        pubThread.join();

        assertTrue(count.get() > 500);
    }

    @Test
    public void testConsumeWithHandler() throws Exception {
        runInJsServer(this::mustBeAtLeast291, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            createDefaultTestStream(jsm);
            JetStream js = nc.jetStream();
            jsPublish(js, SUBJECT, 2500);

            // Pre define a consumer
            ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(NAME).build();
            jsm.addOrUpdateConsumer(STREAM, cc);

            // Consumer[Context]
            ConsumerContext consumerContext = js.getConsumerContext(STREAM, NAME);

            int stopCount = 500;

            CountDownLatch latch = new CountDownLatch(1);
            AtomicInteger atomicCount = new AtomicInteger();
            MessageHandler handler = msg -> {
                msg.ack();
                if (atomicCount.incrementAndGet() == stopCount) {
                    latch.countDown();
                }
            };

            try (MessageConsumer consumer = consumerContext.consume(handler)) {
                latch.await();
                consumer.stop();
                while (!consumer.isFinished()) {
                    Thread.sleep(10);
                }
                assertTrue(atomicCount.get() > 500);
            }
        });
    }

    @Test
    public void testNext() throws Exception {
        runInJsServer(this::mustBeAtLeast291, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            CreateStreamResult csr = createMemoryStream(jsm);
            jsPublish(js, csr.subject, 4);

            String name = name();

            // Pre define a consumer
            ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(name).build();
            jsm.addOrUpdateConsumer(csr.stream, cc);

            // Consumer[Context]
            ConsumerContext consumerContext = js.getConsumerContext(csr.stream, name);
            assertThrows(IllegalArgumentException.class, () -> consumerContext.next(1)); // max wait too small
            assertNotNull(consumerContext.next(1000));
            assertNotNull(consumerContext.next(Duration.ofMillis(1000)));
            assertNotNull(consumerContext.next(null));
            assertNotNull(consumerContext.next());
            assertNull(consumerContext.next(1000));

            StreamContext sc = js.getStreamContext(csr.stream);
            OrderedConsumerContext occ = sc.createOrderedConsumer(new OrderedConsumerConfiguration());
            assertThrows(IllegalArgumentException.class, () -> occ.next(1)); // max wait too small
            assertNotNull(occ.next(1000));
            assertNotNull(occ.next(Duration.ofMillis(1000)));
            assertNotNull(occ.next(null));
            assertNotNull(occ.next());
            assertNull(occ.next(1000));
        });
    }

    @Test
    public void testCoverage() throws Exception {
        runInJsServer(this::mustBeAtLeast291, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            CreateStreamResult csr = createMemoryStream(jsm);
            JetStream js = nc.jetStream();

            // Pre define a consumer
            jsm.addOrUpdateConsumer(csr.stream, ConsumerConfiguration.builder().durable(name(1)).build());
            jsm.addOrUpdateConsumer(csr.stream, ConsumerConfiguration.builder().durable(name(2)).build());
            jsm.addOrUpdateConsumer(csr.stream, ConsumerConfiguration.builder().durable(name(3)).build());
            jsm.addOrUpdateConsumer(csr.stream, ConsumerConfiguration.builder().durable(name(4)).build());

            // Stream[Context]
            StreamContext sctx1 = nc.getStreamContext(csr.stream);
            nc.getStreamContext(csr.stream, JetStreamOptions.DEFAULT_JS_OPTIONS);
            js.getStreamContext(csr.stream);

            // Consumer[Context]
            ConsumerContext cctx1 = nc.getConsumerContext(csr.stream, name(1));
            ConsumerContext cctx2 = nc.getConsumerContext(csr.stream, name(2), JetStreamOptions.DEFAULT_JS_OPTIONS);
            ConsumerContext cctx3 = js.getConsumerContext(csr.stream, name(3));
            ConsumerContext cctx4 = sctx1.getConsumerContext(name(4));
            ConsumerContext cctx5 = sctx1.createOrUpdateConsumer(ConsumerConfiguration.builder().durable(name(5)).build());
            ConsumerContext cctx6 = sctx1.createOrUpdateConsumer(ConsumerConfiguration.builder().durable(name(6)).build());

            after(cctx1.iterate(), name(1), true);
            after(cctx2.iterate(ConsumeOptions.DEFAULT_CONSUME_OPTIONS), name(2), true);
            after(cctx3.consume(m -> {}), name(3), true);
            after(cctx4.consume(ConsumeOptions.DEFAULT_CONSUME_OPTIONS, m -> {}), name(4), true);
            after(cctx5.fetchMessages(1), name(5), false);
            after(cctx6.fetchBytes(1000), name(6), false);
        });
    }

    private void after(MessageConsumer con, String name, boolean doStop) throws Exception {
        ConsumerInfo ci = con.getConsumerInfo();
        assertEquals(name, ci.getName());
        if (doStop) {
            con.stop();
        }
    }

    @Test
    public void testFetchConsumeOptionsBuilder() {
        FetchConsumeOptions fco = FetchConsumeOptions.builder().build();
        assertEquals(DEFAULT_MESSAGE_COUNT, fco.getMaxMessages());
        assertEquals(DEFAULT_EXPIRES_IN_MILLIS, fco.getExpiresInMillis());
        assertEquals(DEFAULT_THRESHOLD_PERCENT, fco.getThresholdPercent());
        assertEquals(0, fco.getMaxBytes());
        assertEquals(DEFAULT_EXPIRES_IN_MILLIS * MAX_IDLE_HEARTBEAT_PERCENT / 100, fco.getIdleHeartbeat());

        fco = FetchConsumeOptions.builder().maxMessages(1000).build();
        assertEquals(1000, fco.getMaxMessages());
        assertEquals(0, fco.getMaxBytes());
        assertEquals(DEFAULT_THRESHOLD_PERCENT, fco.getThresholdPercent());

        fco = FetchConsumeOptions.builder().maxMessages(1000).thresholdPercent(50).build();
        assertEquals(1000, fco.getMaxMessages());
        assertEquals(0, fco.getMaxBytes());
        assertEquals(50, fco.getThresholdPercent());

        fco = FetchConsumeOptions.builder().max(1000, 100).build();
        assertEquals(100, fco.getMaxMessages());
        assertEquals(1000, fco.getMaxBytes());
        assertEquals(DEFAULT_THRESHOLD_PERCENT, fco.getThresholdPercent());

        fco = FetchConsumeOptions.builder().max(1000, 100).thresholdPercent(50).build();
        assertEquals(100, fco.getMaxMessages());
        assertEquals(1000, fco.getMaxBytes());
        assertEquals(50, fco.getThresholdPercent());
    }

    @Test
    public void testConsumeOptionsBuilder() {
        ConsumeOptions co = ConsumeOptions.builder().build();
        assertEquals(DEFAULT_MESSAGE_COUNT, co.getBatchSize());
        assertEquals(DEFAULT_EXPIRES_IN_MILLIS, co.getExpiresInMillis());
        assertEquals(DEFAULT_THRESHOLD_PERCENT, co.getThresholdPercent());
        assertEquals(0, co.getBatchBytes());
        assertEquals(DEFAULT_EXPIRES_IN_MILLIS * MAX_IDLE_HEARTBEAT_PERCENT / 100, co.getIdleHeartbeat());

        co = ConsumeOptions.builder().batchSize(1000).build();
        assertEquals(1000, co.getBatchSize());
        assertEquals(0, co.getBatchBytes());
        assertEquals(DEFAULT_THRESHOLD_PERCENT, co.getThresholdPercent());

        co = ConsumeOptions.builder().batchSize(1000).thresholdPercent(50).build();
        assertEquals(1000, co.getBatchSize());
        assertEquals(0, co.getBatchBytes());
        assertEquals(50, co.getThresholdPercent());

        co = ConsumeOptions.builder().batchBytes(1000).build();
        assertEquals(DEFAULT_MESSAGE_COUNT_WHEN_BYTES, co.getBatchSize());
        assertEquals(1000, co.getBatchBytes());
        assertEquals(DEFAULT_THRESHOLD_PERCENT, co.getThresholdPercent());

        co = ConsumeOptions.builder().thresholdPercent(0).build();
        assertEquals(DEFAULT_THRESHOLD_PERCENT, co.getThresholdPercent());

        co = ConsumeOptions.builder().thresholdPercent(-1).build();
        assertEquals(DEFAULT_THRESHOLD_PERCENT, co.getThresholdPercent());

        co = ConsumeOptions.builder().thresholdPercent(-999).build();
        assertEquals(DEFAULT_THRESHOLD_PERCENT, co.getThresholdPercent());

        co = ConsumeOptions.builder().thresholdPercent(99).build();
        assertEquals(99, co.getThresholdPercent());

        co = ConsumeOptions.builder().thresholdPercent(100).build();
        assertEquals(100, co.getThresholdPercent());

        co = ConsumeOptions.builder().thresholdPercent(101).build();
        assertEquals(100, co.getThresholdPercent());

        co = ConsumeOptions.builder().expiresIn(0).build();
        assertEquals(DEFAULT_EXPIRES_IN_MILLIS, co.getExpiresInMillis());

        co = ConsumeOptions.builder().expiresIn(-1).build();
        assertEquals(DEFAULT_EXPIRES_IN_MILLIS, co.getExpiresInMillis());

        co = ConsumeOptions.builder().expiresIn(-999).build();
        assertEquals(DEFAULT_EXPIRES_IN_MILLIS, co.getExpiresInMillis());

        assertThrows(IllegalArgumentException.class,
            () -> ConsumeOptions.builder().expiresIn(MIN_EXPIRES_MILLS - 1).build());
    }

    public static class OrderedPullTestDropSimulator extends OrderedPullMessageManager {
        public OrderedPullTestDropSimulator(NatsConnection conn, NatsJetStream js, String stream, SubscribeOptions so, ConsumerConfiguration serverCC, boolean queueMode, boolean syncMode) {
            super(conn, js, stream, so, serverCC, syncMode);
        }

        @Override
        protected Boolean beforeQueueProcessorImpl(NatsMessage msg) {
            if (msg.isJetStream()) {
                long ss = msg.metaData().streamSequence();
                long cs = msg.metaData().consumerSequence();
                if ((ss == 2 && cs == 2) || (ss == 5 && cs == 4)) {
                    return false;
                }
            }

            return super.beforeQueueProcessorImpl(msg);
        }
    }

    public static class OrderedPullNextTestDropSimulator extends OrderedPullMessageManager {
        public OrderedPullNextTestDropSimulator(NatsConnection conn, NatsJetStream js, String stream, SubscribeOptions so, ConsumerConfiguration serverCC, boolean queueMode, boolean syncMode) {
            super(conn, js, stream, so, serverCC, syncMode);
        }

        // these have to be static or the test keeps repeating
        static boolean ss2 = true;
        static boolean ss5 = true;

        @Override
        protected Boolean beforeQueueProcessorImpl(NatsMessage msg) {
            if (msg.isJetStream()) {
                long ss = msg.metaData().streamSequence();
                if (ss == 2 && ss2) {
                    ss2 = false;
                    return false;
                }
                if (ss == 5 && ss5) {
                    ss5 = false;
                    return false;
                }
            }

            return super.beforeQueueProcessorImpl(msg);
        }
    }

    @Test
    public void testOrderedBehaviors() throws Exception {
        runInJsServer(this::mustBeAtLeast291, nc -> {
            // Setup
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            // Get this in place before subscriptions are made
            ((NatsJetStream)js)._pullOrderedMessageManagerFactory = OrderedPullNextTestDropSimulator::new;

            CreateStreamResult csr = createMemoryStream(jsm);
            StreamContext sc = js.getStreamContext(csr.stream);
            jsPublish(js, csr.subject, 101, 6);
            testOrderedBehaviorNext(sc, new OrderedConsumerConfiguration().filterSubject(csr.subject));
            try { jsm.deleteStream(csr.stream); } catch (Exception ignore) {};

            // Get this in place before subscriptions are made
            ((NatsJetStream)js)._pullOrderedMessageManagerFactory = OrderedPullTestDropSimulator::new;

            csr = createMemoryStream(jsm);
            sc = js.getStreamContext(csr.stream);
            jsPublish(js, csr.subject, 101, 6);
            testOrderedBehaviorFetch(sc, new OrderedConsumerConfiguration().filterSubject(csr.subject));
            try { jsm.deleteStream(csr.stream); } catch (Exception ignore) {};

            csr = createMemoryStream(jsm);
            sc = js.getStreamContext(csr.stream);
            jsPublish(js, csr.subject, 101, 6);
            testOrderedBehaviorIterable(sc, new OrderedConsumerConfiguration().filterSubject(csr.subject));
            try { jsm.deleteStream(csr.stream); } catch (Exception ignore) {};
        });
    }

    private static void testOrderedBehaviorNext(StreamContext sc, OrderedConsumerConfiguration occ) throws Exception {
        OrderedConsumerContext ctx = sc.createOrderedConsumer(occ);
        // Loop through the messages to make sure I get stream sequence 1 to 6
        int expectedStreamSeq = 1;
        while (expectedStreamSeq <= 6) {
            Message m = ctx.next(1000);
            if (m != null) {
                assertEquals(expectedStreamSeq, m.metaData().streamSequence());
                assertEquals(1, m.metaData().consumerSequence());
                ++expectedStreamSeq;
            }
        }
    }

    private static void testOrderedBehaviorFetch(StreamContext sc, OrderedConsumerConfiguration occ) throws Exception {
        OrderedConsumerContext ctx = sc.createOrderedConsumer(occ);
        try (FetchConsumer fcon = ctx.fetchMessages(6)) {
            // Loop through the messages to make sure I get stream sequence 1 to 6
            int expectedStreamSeq = 1;
            while (expectedStreamSeq <= 6) {
                Message m = fcon.nextMessage();
                if (m != null) {
                    assertEquals(expectedStreamSeq, m.metaData().streamSequence());
                    assertEquals(EXPECTED_CON_SEQ_NUMS[expectedStreamSeq-1], m.metaData().consumerSequence());
                    ++expectedStreamSeq;
                }
            }
        }
    }

    private static void testOrderedBehaviorIterable(StreamContext sc, OrderedConsumerConfiguration occ) throws Exception {
        OrderedConsumerContext ctx = sc.createOrderedConsumer(occ);
        try (IterableConsumer icon = ctx.iterate()) {
            // Loop through the messages to make sure I get stream sequence 1 to 6
            int expectedStreamSeq = 1;
            while (expectedStreamSeq <= 6) {
                Message m = icon.nextMessage(Duration.ofSeconds(1)); // use duration version here for coverage
                if (m != null) {
                    assertEquals(expectedStreamSeq, m.metaData().streamSequence());
                    assertEquals(EXPECTED_CON_SEQ_NUMS[expectedStreamSeq-1], m.metaData().consumerSequence());
                    ++expectedStreamSeq;
                }
            }
        }
    }

    @Test
    public void testOrderedConsume() throws Exception {
        runInJsServer(this::mustBeAtLeast291, nc -> {
            // Setup
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            String subject = subject(222);
            createMemoryStream(jsm, stream(222), subject);

            StreamContext sc = js.getStreamContext(stream(222));

            // Get this in place before subscriptions are made
            ((NatsJetStream)js)._pullOrderedMessageManagerFactory = OrderedPullTestDropSimulator::new;

            CountDownLatch msgLatch = new CountDownLatch(6);
            AtomicInteger received = new AtomicInteger();
            AtomicLong[] ssFlags = new AtomicLong[6];
            AtomicLong[] csFlags = new AtomicLong[6];
            MessageHandler handler = hmsg -> {
                int i = received.incrementAndGet() - 1;
                ssFlags[i] = new AtomicLong(hmsg.metaData().streamSequence());
                csFlags[i] = new AtomicLong(hmsg.metaData().consumerSequence());
                msgLatch.countDown();
            };

            OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration().filterSubject(subject);
            OrderedConsumerContext ctx = sc.createOrderedConsumer(occ);
            try (MessageConsumer mcon = ctx.consume(handler)) {
                jsPublish(js, subject, 201, 6);

                // wait for the messages
                awaitAndAssert(msgLatch);

                // Loop through the messages to make sure I get stream sequence 1 to 6
                int expectedStreamSeq = 1;
                while (expectedStreamSeq <= 6) {
                    int idx = expectedStreamSeq - 1;
                    assertEquals(expectedStreamSeq, ssFlags[idx].get());
                    assertEquals(EXPECTED_CON_SEQ_NUMS[idx], csFlags[idx].get());
                    ++expectedStreamSeq;
                }
            }
        });
    }

    @Test
    public void testOrderedMultipleWays() throws Exception {
        runInJsServer(this::mustBeAtLeast291, nc -> {
            // Setup
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            String subject = subject(333);
            createMemoryStream(jsm, stream(333), subject);

            StreamContext sc = js.getStreamContext(stream(333));

            OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration().filterSubject(subject);
            OrderedConsumerContext ctx = sc.createOrderedConsumer(occ);

            // can't do others while doing next
            CountDownLatch latch = new CountDownLatch(1);
            new Thread(() -> {
                try {
                    // make sure there is enough time to call other methods.
                    assertNull(ctx.next(2000));
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
                finally {
                    latch.countDown();
                }
            }).start();

            Thread.sleep(100); // make sure there is enough time for the thread to start and get into the next method
            validateCantCallOtherMethods(ctx);

            //noinspection ResultOfMethodCallIgnored
            latch.await(3000, TimeUnit.MILLISECONDS);

            for (int x = 0 ; x < 10_000; x++) {
                js.publish(subject, ("multiple" + x).getBytes());
            }

            // can do others now
            Message m = ctx.next(1000);
            assertNotNull(m);
            assertEquals(1, m.metaData().streamSequence());

            // can't do others while doing next
            int seq = 2;
            try (FetchConsumer fc = ctx.fetchMessages(5)) {
                while (seq <= 6) {
                    m = fc.nextMessage();
                    assertNotNull(m);
                    assertEquals(seq, m.metaData().streamSequence());
                    assertFalse(fc.isFinished());
                    validateCantCallOtherMethods(ctx);
                    seq++;
                }

                assertNull(fc.nextMessage());
                assertTrue(fc.isFinished());
                assertNull(fc.nextMessage()); // just some coverage
            }

            // can do others now
            m = ctx.next(1000);
            assertNotNull(m);
            assertEquals(seq++, m.metaData().streamSequence());

            // can't do others while doing iterate
            ConsumeOptions copts = ConsumeOptions.builder().batchSize(10).build();
            try (IterableConsumer ic = ctx.iterate(copts)) {
                ic.stop();
                m = ic.nextMessage(1000);
                while (m != null) {
                    assertEquals(seq, m.metaData().streamSequence());
                    if (!ic.isFinished()) {
                        validateCantCallOtherMethods(ctx);
                    }
                    ++seq;
                    m = ic.nextMessage(1000);
                }
            }

            // can do others now
            m = ctx.next(1000);
            assertNotNull(m);
            assertEquals(seq++, m.metaData().streamSequence());

            int last = Math.min(seq + 10, 9999);
            try (FetchConsumer fc = ctx.fetchMessages(last - seq)) {
                while (seq < last) {
                    fc.stop();
                    m = fc.nextMessage();
                    assertNotNull(m);
                    assertEquals(seq, m.metaData().streamSequence());
                    assertFalse(fc.isFinished());
                    validateCantCallOtherMethods(ctx);
                    seq++;
                }
            }
        });
    }

    private static void validateCantCallOtherMethods(OrderedConsumerContext ctx) {
        assertThrows(IOException.class, () -> ctx.next(1000));
        assertThrows(IOException.class, () -> ctx.fetchMessages(1));
        assertThrows(IOException.class, () -> ctx.consume(null));
    }

    @Test
    public void testOrderedConsumerBuilder() {
        OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration();
        assertEquals(">", occ.getFilterSubject());
        assertNull(occ.getDeliverPolicy());
        assertEquals(ConsumerConfiguration.LONG_UNSET, occ.getStartSequence());
        assertNull(occ.getStartTime());
        assertNull(occ.getReplayPolicy());
        assertNull(occ.getHeadersOnly());

        // nulls
        occ = new OrderedConsumerConfiguration()
            .filterSubject(null)
            .deliverPolicy(null)
            .replayPolicy(null)
            .headersOnly(null);
        assertEquals(">", occ.getFilterSubject());
        assertNull(occ.getDeliverPolicy());
        assertEquals(ConsumerConfiguration.LONG_UNSET, occ.getStartSequence());
        assertNull(occ.getStartTime());
        assertNull(occ.getReplayPolicy());
        assertNull(occ.getHeadersOnly());

        // values that set to default
        occ = new OrderedConsumerConfiguration()
            .filterSubject("")
            .startSequence(-42)
            .headersOnly(false);
        assertEquals(">", occ.getFilterSubject());
        assertNull(occ.getDeliverPolicy());
        assertEquals(ConsumerConfiguration.LONG_UNSET, occ.getStartSequence());
        assertNull(occ.getStartTime());
        assertNull(occ.getReplayPolicy());
        assertNull(occ.getHeadersOnly());

        // values
        ZonedDateTime zdt = ZonedDateTime.now();
        occ = new OrderedConsumerConfiguration()
            .filterSubject("fs")
            .deliverPolicy(DeliverPolicy.All)
            .startSequence(42)
            .startTime(zdt)
            .replayPolicy(ReplayPolicy.Original)
            .headersOnly(true);
        assertEquals("fs", occ.getFilterSubject());
        assertEquals(DeliverPolicy.All, occ.getDeliverPolicy());
        assertEquals(42, occ.getStartSequence());
        assertEquals(zdt, occ.getStartTime());
        assertEquals(ReplayPolicy.Original, occ.getReplayPolicy());
        assertTrue(occ.getHeadersOnly());
    }
}
