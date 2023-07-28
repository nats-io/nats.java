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
import java.util.concurrent.CountDownLatch;
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

            assertThrows(JetStreamApiException.class, () -> nc.streamContext(STREAM));
            assertThrows(JetStreamApiException.class, () -> nc.streamContext(STREAM, JetStreamOptions.DEFAULT_JS_OPTIONS));
            assertThrows(JetStreamApiException.class, () -> js.streamContext(STREAM));

            createMemoryStream(jsm, STREAM, SUBJECT);
            StreamContext streamContext = nc.streamContext(STREAM);
            assertEquals(STREAM, streamContext.getStreamName());
            _testStreamContext(js, streamContext);

            jsm.deleteStream(STREAM);

            createMemoryStream(jsm, STREAM, SUBJECT);
            streamContext = js.streamContext(STREAM);
            assertEquals(STREAM, streamContext.getStreamName());
            _testStreamContext(js, streamContext);
        });
    }

    private static void _testStreamContext(JetStream js, StreamContext streamContext) throws IOException, JetStreamApiException {
        assertThrows(JetStreamApiException.class, () -> streamContext.consumerContext(DURABLE));
        assertThrows(JetStreamApiException.class, () -> streamContext.deleteConsumer(DURABLE));

        ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(DURABLE).build();
        ConsumerContext consumerContext = streamContext.createOrUpdateConsumer(cc);
        ConsumerInfo ci = consumerContext.getConsumerInfo();
        assertEquals(STREAM, ci.getStreamName());
        assertEquals(DURABLE, ci.getName());

        ci = streamContext.getConsumerInfo(DURABLE);
        assertNotNull(ci);
        assertEquals(STREAM, ci.getStreamName());
        assertEquals(DURABLE, ci.getName());

        assertEquals(1, streamContext.getConsumerNames().size());

        assertEquals(1, streamContext.getConsumers().size());
        assertNotNull(streamContext.consumerContext(DURABLE));
        streamContext.deleteConsumer(DURABLE);

        assertThrows(JetStreamApiException.class, () -> streamContext.consumerContext(DURABLE));
        assertThrows(JetStreamApiException.class, () -> streamContext.deleteConsumer(DURABLE));

        // coverage
        js.publish(SUBJECT, "one".getBytes());
        js.publish(SUBJECT, "two".getBytes());
        js.publish(SUBJECT, "three".getBytes());
        js.publish(SUBJECT, "four".getBytes());
        js.publish(SUBJECT, "five".getBytes());
        js.publish(SUBJECT, "six".getBytes());

        assertTrue(streamContext.deleteMessage(3));
        assertTrue(streamContext.deleteMessage(4, true));

        MessageInfo mi = streamContext.getMessage(1);
        assertEquals(1, mi.getSeq());

        mi = streamContext.getFirstMessage(SUBJECT);
        assertEquals(1, mi.getSeq());

        mi = streamContext.getLastMessage(SUBJECT);
        assertEquals(6, mi.getSeq());

        mi = streamContext.getNextMessage(3, SUBJECT);
        assertEquals(5, mi.getSeq());

        assertNotNull(streamContext.getStreamInfo());
        assertNotNull(streamContext.getStreamInfo(StreamInfoOptions.builder().build()));

        streamContext.purge(PurgeOptions.builder().sequence(5).build());
        assertThrows(JetStreamApiException.class, () -> streamContext.getMessage(1));

        mi = streamContext.getFirstMessage(SUBJECT);
        assertEquals(5, mi.getSeq());

        streamContext.purge();
        assertThrows(JetStreamApiException.class, () -> streamContext.getFirstMessage(SUBJECT));
    }

    @Test
    public void testFetch() throws Exception {
        runInJsServer(this::mustBeAtLeast291, nc -> {
            createDefaultTestStream(nc);
            JetStream js = nc.jetStream();
            for (int x = 1; x <= 20; x++) {
                js.publish(SUBJECT, ("test-fetch-msg-" + x).getBytes());
            }

            // 1. Different fetch sizes demonstrate expiration behavior

            // 1A. equal number of messages than the fetch size
            _testFetch("1A", nc, 20, 0, 20);

            // 1B. more messages than the fetch size
            _testFetch("1B", nc, 10, 0, 10);

            // 1C. fewer messages than the fetch size
            _testFetch("1C", nc, 40, 0, 40);

            // 1D. simple-consumer-40msgs was created in 1C and has no messages available
            _testFetch("1D", nc, 40, 0, 40);

            // 2. Different max bytes sizes demonstrate expiration behavior
            //    - each test message is approximately 100 bytes

            // 2A. max bytes is reached before message count
            _testFetch("2A", nc, 0, 750, 20);

            // 2B. fetch size is reached before byte count
            _testFetch("2B", nc, 10, 1500, 10);

            // 2C. fewer bytes than the byte count
            _testFetch("2C", nc, 0, 3000, 40);
        });
    }

    private static void _testFetch(String label, Connection nc, int maxMessages, int maxBytes, int testAmount) throws Exception {
        JetStreamManagement jsm = nc.jetStreamManagement();
        JetStream js = nc.jetStream();

        String name = generateConsumerName(maxMessages, maxBytes);

        // Pre define a consumer
        ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(name).build();
        jsm.addOrUpdateConsumer(STREAM, cc);

        // Consumer[Context]
        ConsumerContext consumerContext = js.consumerContext(STREAM, name);

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
            ConsumerContext consumerContext = js.consumerContext(STREAM, DURABLE);

            int stopCount = 500;
            // create the consumer then use it
            try (IterableConsumer consumer = consumerContext.startIterate()) {
                _testIterable(js, stopCount, consumer);
            }

            // coverage
            IterableConsumer consumer = consumerContext.startIterate(ConsumeOptions.DEFAULT_CONSUME_OPTIONS);
            consumer.close();
            assertThrows(IllegalArgumentException.class, () -> consumerContext.startIterate((ConsumeOptions) null));
        });
    }

    @Test
    public void testOrderedIterableConsumerBasic() throws Exception {
        runInJsServer(this::mustBeAtLeast291, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            createDefaultTestStream(jsm);
            JetStream js = nc.jetStream();
            StreamContext sc = nc.streamContext(STREAM);

            int stopCount = 500;
            OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration().filterSubject(SUBJECT);
            try (IterableConsumer consumer = sc.startOrderedIterate(occ)) {
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
                consumer.stop(200);

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
            ConsumerContext consumerContext = js.consumerContext(STREAM, NAME);

            int stopCount = 500;

            CountDownLatch latch = new CountDownLatch(1);
            AtomicInteger atomicCount = new AtomicInteger();
            MessageHandler handler = msg -> {
                msg.ack();
                if (atomicCount.incrementAndGet() == stopCount) {
                    latch.countDown();
                }
            };

            try (MessageConsumer consumer = consumerContext.startConsume(handler)) {
                latch.await();
                consumer.stop(200);
                assertTrue(atomicCount.get() > 500);
            }
        });
    }

    @Test
    public void testNext() throws Exception {
        runInJsServer(this::mustBeAtLeast291, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            createDefaultTestStream(jsm);
            JetStream js = nc.jetStream();
            jsPublish(js, SUBJECT, 4);

            // Pre define a consumer
            ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(NAME).build();
            jsm.addOrUpdateConsumer(STREAM, cc);

            // Consumer[Context]
            ConsumerContext consumerContext = js.consumerContext(STREAM, NAME);

            assertThrows(IllegalArgumentException.class, () -> consumerContext.next(1));
            assertNotNull(consumerContext.next(1000));
            assertNotNull(consumerContext.next(Duration.ofMillis(1000)));
            assertNotNull(consumerContext.next(null));
            assertNotNull(consumerContext.next());
            assertNull(consumerContext.next(1000));
        });
    }

    @Test
    public void testCoverage() throws Exception {
        runInJsServer(this::mustBeAtLeast291, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            createDefaultTestStream(jsm);
            JetStream js = nc.jetStream();

            // Pre define a consumer
            jsm.addOrUpdateConsumer(STREAM, ConsumerConfiguration.builder().durable(name(1)).build());
            jsm.addOrUpdateConsumer(STREAM, ConsumerConfiguration.builder().durable(name(2)).build());
            jsm.addOrUpdateConsumer(STREAM, ConsumerConfiguration.builder().durable(name(3)).build());
            jsm.addOrUpdateConsumer(STREAM, ConsumerConfiguration.builder().durable(name(4)).build());

            // Stream[Context]
            StreamContext sctx1 = nc.streamContext(STREAM);
            nc.streamContext(STREAM, JetStreamOptions.DEFAULT_JS_OPTIONS);
            js.streamContext(STREAM);

            // Consumer[Context]
            ConsumerContext cctx1 = nc.consumerContext(STREAM, name(1));
            ConsumerContext cctx2 = nc.consumerContext(STREAM, name(2), JetStreamOptions.DEFAULT_JS_OPTIONS);
            ConsumerContext cctx3 = js.consumerContext(STREAM, name(3));
            ConsumerContext cctx4 = sctx1.consumerContext(name(4));
            ConsumerContext cctx5 = sctx1.createOrUpdateConsumer(ConsumerConfiguration.builder().durable(name(5)).build());
            ConsumerContext cctx6 = sctx1.createOrUpdateConsumer(ConsumerConfiguration.builder().durable(name(6)).build());

            closeConsumer(cctx1.startIterate(), name(1), true);
            closeConsumer(cctx2.startIterate(ConsumeOptions.DEFAULT_CONSUME_OPTIONS), name(2), true);
            closeConsumer(cctx3.startConsume(m -> {}), name(3), true);
            closeConsumer(cctx4.startConsume(m -> {}, ConsumeOptions.DEFAULT_CONSUME_OPTIONS), name(4), true);
            closeConsumer(cctx5.fetchMessages(1), name(5), false);
            closeConsumer(cctx6.fetchBytes(1000), name(6), false);
        });
    }

    private void closeConsumer(MessageConsumer con, String name, boolean doStop) throws Exception {
        ConsumerInfo ci = con.getConsumerInfo();
        assertEquals(name, ci.getName());
        if (doStop) {
            con.stop(100);
        }
    }

    @Test
    public void testFetchConsumeOptionsBuilder() {
        FetchConsumeOptions fco = FetchConsumeOptions.builder().build();
        assertEquals(DEFAULT_MESSAGE_COUNT, fco.getMaxMessages());
        assertEquals(DEFAULT_EXPIRES_IN_MILLIS, fco.getExpiresIn());
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
        assertEquals(DEFAULT_EXPIRES_IN_MILLIS, co.getExpiresIn());
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
        assertEquals(DEFAULT_EXPIRES_IN_MILLIS, co.getExpiresIn());

        co = ConsumeOptions.builder().expiresIn(-1).build();
        assertEquals(DEFAULT_EXPIRES_IN_MILLIS, co.getExpiresIn());

        co = ConsumeOptions.builder().expiresIn(-999).build();
        assertEquals(DEFAULT_EXPIRES_IN_MILLIS, co.getExpiresIn());

        assertThrows(IllegalArgumentException.class,
            () -> ConsumeOptions.builder().expiresIn(MIN_EXPIRES_MILLS - 1).build());
    }

    public static class PullOrderedTestDropSimulator extends PullOrderedMessageManager {
        public PullOrderedTestDropSimulator(NatsConnection conn, NatsJetStream js, String stream, SubscribeOptions so, ConsumerConfiguration serverCC, boolean queueMode, boolean syncMode) {
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

    @Test
    public void testOrderedIterable() throws Exception {
        runInJsServer(this::mustBeAtLeast291, nc -> {
            // Setup
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            String subject = subject(111);
            createMemoryStream(jsm, stream(111), subject);

            StreamContext sc = js.streamContext(stream(111));

            // Get this in place before any subscriptions are made
            ((NatsJetStream)js)._pullOrderedMessageManagerFactory = PullOrderedTestDropSimulator::new;

            // Published messages will be intercepted by the OrderedTestDropSimulator
            new Thread(() -> {
                try {
                    Thread.sleep(1000); // give the consumer time to get setup before publishing
                    jsPublish(js, subject, 101, 6);
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }).start();

            OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration().filterSubject(subject);
            try (IterableConsumer icon = sc.startOrderedIterate(occ)) {
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
        });
    }

    @Test
    public void testOrderedConsume() throws Exception {
        runInJsServer(this::mustBeAtLeast291, nc -> {
            // Setup
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            String subject = subject(222);
            createMemoryStream(jsm, stream(222), subject);

            StreamContext sc = js.streamContext(stream(222));

            // Get this in place before any subscriptions are made
            ((NatsJetStream)js)._pullOrderedMessageManagerFactory = PullOrderedTestDropSimulator::new;

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
            try (MessageConsumer mcon = sc.startOrderedConsume(occ, handler)) {
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
}
