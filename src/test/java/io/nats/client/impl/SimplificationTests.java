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
import io.nats.client.support.*;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.*;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.nats.client.BaseConsumeOptions.*;
import static org.junit.jupiter.api.Assertions.*;

@Isolated
public class SimplificationTests extends JetStreamTestBase {

    @Test
    public void testStreamContext() throws Exception {
        jsServer.run(TestBase::atLeast2_9_1, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            assertThrows(JetStreamApiException.class, () -> nc.getStreamContext(stream()));
            assertThrows(JetStreamApiException.class, () -> nc.getStreamContext(stream(), JetStreamOptions.DEFAULT_JS_OPTIONS));
            assertThrows(JetStreamApiException.class, () -> js.getStreamContext(stream()));

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            StreamContext streamContext = nc.getStreamContext(tsc.stream);
            assertEquals(tsc.stream, streamContext.getStreamName());
            _testStreamContext(js, tsc, streamContext);

            tsc = new TestingStreamContainer(jsm);
            streamContext = js.getStreamContext(tsc.stream);
            assertEquals(tsc.stream, streamContext.getStreamName());
            _testStreamContext(js, tsc, streamContext);
        });
    }

    private void _testStreamContext(JetStream js, TestingStreamContainer tsc, StreamContext streamContext) throws IOException, JetStreamApiException {
        String durable = durable();
        assertThrows(JetStreamApiException.class, () -> streamContext.getConsumerContext(durable));
        assertThrows(JetStreamApiException.class, () -> streamContext.deleteConsumer(durable));

        ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(durable).build();
        ConsumerContext consumerContext = streamContext.createOrUpdateConsumer(cc);
        ConsumerInfo ci = consumerContext.getConsumerInfo();
        assertEquals(tsc.stream, ci.getStreamName());
        assertEquals(durable, ci.getName());

        ci = streamContext.getConsumerInfo(durable);
        assertNotNull(ci);
        assertEquals(tsc.stream, ci.getStreamName());
        assertEquals(durable, ci.getName());

        assertEquals(1, streamContext.getConsumerNames().size());

        assertEquals(1, streamContext.getConsumers().size());
        consumerContext = streamContext.getConsumerContext(durable);
        assertNotNull(consumerContext);
        assertEquals(durable, consumerContext.getConsumerName());

        ci = consumerContext.getConsumerInfo();
        assertNotNull(ci);
        assertEquals(tsc.stream, ci.getStreamName());
        assertEquals(durable, ci.getName());

        ci = consumerContext.getCachedConsumerInfo();
        assertNotNull(ci);
        assertEquals(tsc.stream, ci.getStreamName());
        assertEquals(durable, ci.getName());

        streamContext.deleteConsumer(durable);

        assertThrows(JetStreamApiException.class, () -> streamContext.getConsumerContext(durable));
        assertThrows(JetStreamApiException.class, () -> streamContext.deleteConsumer(durable));

        // coverage
        js.publish(tsc.subject(), "one".getBytes());
        js.publish(tsc.subject(), "two".getBytes());
        js.publish(tsc.subject(), "three".getBytes());
        js.publish(tsc.subject(), "four".getBytes());
        js.publish(tsc.subject(), "five".getBytes());
        js.publish(tsc.subject(), "six".getBytes());

        assertTrue(streamContext.deleteMessage(3));
        assertTrue(streamContext.deleteMessage(4, true));

        MessageInfo mi = streamContext.getMessage(1);
        assertEquals(1, mi.getSeq());

        mi = streamContext.getFirstMessage(tsc.subject());
        assertEquals(1, mi.getSeq());

        mi = streamContext.getLastMessage(tsc.subject());
        assertEquals(6, mi.getSeq());

        mi = streamContext.getNextMessage(3, tsc.subject());
        assertEquals(5, mi.getSeq());

        assertNotNull(streamContext.getStreamInfo());
        assertNotNull(streamContext.getStreamInfo(StreamInfoOptions.builder().build()));

        streamContext.purge(PurgeOptions.builder().sequence(5).build());
        assertThrows(JetStreamApiException.class, () -> streamContext.getMessage(1));
    }

    private void validateConsumerName(BaseConsumerContext bcc, MessageConsumer consumer, String consumerName) throws IOException, JetStreamApiException {
        assertEquals(consumerName, bcc.getConsumerName());
        if (consumer != null) {
            assertNotNull(consumer.getCachedConsumerInfo());
            assertEquals(consumerName, consumer.getConsumerName());
            assertEquals(consumerName, consumer.getConsumerInfo().getName());
        }
    }

    private String validateConsumerNameForOrdered(BaseConsumerContext bcc, MessageConsumer consumer, String prefix) throws IOException, JetStreamApiException {
        String bccConsumerName = bcc.getConsumerName();
        if (prefix == null) {
            assertNotNull(bccConsumerName);
        }
        else {
            assertTrue(bccConsumerName.startsWith(prefix));
        }

        if (consumer != null) {
            if (prefix == null) {
                assertNotNull(consumer.getConsumerName());
                assertNotNull(consumer.getConsumerInfo().getName());
            }
            else {
                assertTrue(consumer.getConsumerName().startsWith(prefix));
                assertTrue(consumer.getConsumerInfo().getName().startsWith(prefix));
            }
        }
        return bccConsumerName;
    }

    static int FETCH_EPHEMERAL = 1;
    static int FETCH_DURABLE = 2;
    static int FETCH_ORDERED = 3;
    @Test
    public void testFetch() throws Exception {
        jsServer.run(TestBase::atLeast2_9_1, nc -> {
            TestingStreamContainer tsc = new TestingStreamContainer(nc);
            JetStream js = nc.jetStream();
            for (int x = 1; x <= 20; x++) {
                js.publish(tsc.subject(), ("test-fetch-msg-" + x).getBytes());
            }

            for (int f = FETCH_EPHEMERAL; f <= FETCH_ORDERED; f++) {
                // 1. Different fetch sizes demonstrate expiration behavior

                // 1A. equal number of messages to the fetch size
                _testFetch("1A", nc, tsc, 20, 0, 20, f, false);

                // 1B. more messages than the fetch size
                _testFetch("1B", nc, tsc, 10, 0, 10, f, false);

                // 1C. fewer messages than the fetch size
                _testFetch("1C", nc, tsc, 40, 0, 40, f, false);

                // 1D. simple-consumer-40msgs was created in 1C and has no messages available
                _testFetch("1D", nc, tsc, 40, 0, 40, f, false);

                // 2. Different max bytes sizes demonstrate expiration behavior
                //    - each test message is approximately 100 bytes

                // 2A. max bytes are reached before message count
                _testFetch("2A", nc, tsc, 0, 750, 20, f, false);

                // 2B. fetch size is reached before byte count
                _testFetch("2B", nc, tsc, 10, 1500, 10, f, false);

                if (f == FETCH_DURABLE) {
                    // this is long-running, so don't want to test every time
                    // 2C. fewer bytes than the byte count
                    _testFetch("2C", nc, tsc, 0, 3000, 40, f, false);
                }
                else if (f == FETCH_ORDERED) {
                    // just to get coverage of testing with a consumer name prefix
                    _testFetch("1A", nc, tsc, 20, 0, 20, f, true);
                    _testFetch("2A", nc, tsc, 0, 750, 20, f, true);
                }

            }
        });
    }

    private void _testFetch(String label, Connection nc, TestingStreamContainer tsc, int maxMessages, int maxBytes, int testAmount, int fetchType, boolean useConsumerPrefix) throws Exception {
        JetStreamManagement jsm = nc.jetStreamManagement();
        JetStream js = nc.jetStream();

        StreamContext ctx = js.getStreamContext(tsc.stream);

        String consumerName = null;
        String consumerNamePrefix = null;
        BaseConsumerContext consumerContext;
        if (fetchType == FETCH_ORDERED) {
            OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration();
            if (useConsumerPrefix) {
                consumerNamePrefix = prefix();
                occ.consumerNamePrefix(consumerNamePrefix);
            }
            consumerContext = ctx.createOrderedConsumer(occ);
            assertNull(consumerContext.getConsumerName());
        }
        else {
            // Pre define a consumer
            consumerName = generateConsumerName(maxMessages, maxBytes);
            ConsumerConfiguration.Builder builder = ConsumerConfiguration.builder();
            ConsumerConfiguration cc;
            if (fetchType == FETCH_DURABLE) {
                consumerName = consumerName + "D";
                cc = builder.durable(consumerName).build();
            }
            else {
                consumerName = consumerName + "E";
                cc = builder.name(consumerName).inactiveThreshold(10_000).build();
            }
            jsm.addOrUpdateConsumer(tsc.stream, cc);

            // Consumer[Context]
            consumerContext = ctx.getConsumerContext(consumerName);
            assertEquals(consumerName, consumerContext.getConsumerName());
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
            if (fetchType == FETCH_ORDERED) {
                validateConsumerNameForOrdered(consumerContext, consumer, consumerNamePrefix);
            }
            else {
                validateConsumerName(consumerContext, consumer, consumerName);
            }
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

    private String generateConsumerName(int maxMessages, int maxBytes) {
        return maxBytes == 0
            ? variant() + "-" + maxMessages + "msgs"
            : variant() + "-" + maxBytes + "bytes-" + maxMessages + "msgs";
    }

    @Test
    public void testFetchNoWaitPlusExpires() throws Exception {
        jsServer.run(TestBase::atLeast2_9_1, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            JetStream js = nc.jetStream();

            jsm.addOrUpdateConsumer(tsc.stream, ConsumerConfiguration.builder()
                .name(tsc.consumerName())
                .inactiveThreshold(100000) // I could have used a durable, but this is long enough for the test
                .filterSubject(tsc.subject())
                .build());

            ConsumerContext cc = nc.getConsumerContext(tsc.stream, tsc.consumerName());
            FetchConsumeOptions fco = FetchConsumeOptions.builder().maxMessages(10).noWait().build();

            // No Wait, No Messages
            FetchConsumer fc = cc.fetch(fco);
            int count = readMessages(fc);
            assertEquals(0, count); // no messages

            // No Wait, One Message
            js.publish(tsc.subject(), "DATA-A".getBytes());
            fc = cc.fetch(fco);
            count = readMessages(fc);
            assertEquals(1, count); // 1 message

            // No Wait, Two Messages
            js.publish(tsc.subject(), "DATA-B".getBytes());
            js.publish(tsc.subject(), "DATA-C".getBytes());
            fc = cc.fetch(fco);
            count = readMessages(fc);
            assertEquals(2, count); // 2 messages

            // With Expires, No Messages
            fco = FetchConsumeOptions.builder().maxMessages(10).noWaitExpiresIn(1000).build();
            fc = cc.fetch(fco);
            count = readMessages(fc);
            assertEquals(0, count); // 0 messages

            // With Expires, One to Three Message
            fco = FetchConsumeOptions.builder().maxMessages(10).noWaitExpiresIn(1000).build();
            fc = cc.fetch(fco);
            js.publish(tsc.subject(), "DATA-D".getBytes());
            js.publish(tsc.subject(), "DATA-E".getBytes());
            js.publish(tsc.subject(), "DATA-F".getBytes());
            count = readMessages(fc);

            // With Long (Default) Expires, Leftovers
            long left = 3 - count;
            fc = cc.fetch(fco);
            count = readMessages(fc);
            assertEquals(left, count);
        });
    }

    private int readMessages(FetchConsumer fc) throws InterruptedException, JetStreamStatusCheckedException {
        int count = 0;
        while (!fc.isFinished()) {
            Message m = fc.nextMessage();
            if (m != null) {
                m.ack();
                ++count;
            }
        }
        return count;
    }

    @Test
    public void testIterableConsumer() throws Exception {
        jsServer.run(TestBase::atLeast2_9_1, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            JetStream js = nc.jetStream();

            // Pre define a consumer
            ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(tsc.consumerName()).build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);

            // Consumer[Context]
            ConsumerContext consumerContext = js.getConsumerContext(tsc.stream, tsc.consumerName());
            validateConsumerName(consumerContext, null, tsc.consumerName());

            int stopCount = 500;
            // create the consumer then use it
            try (IterableConsumer consumer = consumerContext.iterate()) {
                validateConsumerName(consumerContext, consumer, tsc.consumerName());
                _testIterableBasic(js, stopCount, consumer, tsc.subject());
            }

            // coverage
            IterableConsumer consumer = consumerContext.iterate(ConsumeOptions.DEFAULT_CONSUME_OPTIONS);
            validateConsumerName(consumerContext, consumer, tsc.consumerName());
            consumer.close();
            assertThrows(IllegalArgumentException.class, () -> consumerContext.iterate(null));
        });
    }

    @Test
    public void testOrderedConsumerDeliverPolices() throws Exception {
        jsServer.run(TestBase::atLeast2_9_1, nc -> {
            // Setup
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);

            jsPublish(js, tsc.subject(), 101, 3, 100);
            ZonedDateTime startTime = getStartTimeFirstMessage(js, tsc);

            StreamContext sctx = nc.getStreamContext(tsc.stream);

            // test a start time
            OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration()
                .filterSubject(tsc.subject())
                .deliverPolicy(DeliverPolicy.ByStartTime)
                .startTime(startTime);
            OrderedConsumerContext occtx = sctx.createOrderedConsumer(occ);
            try (IterableConsumer consumer = occtx.iterate()) {
                Message m = consumer.nextMessage(1000);
                assertEquals(2, m.metaData().streamSequence());
            }

            // test a start sequence
            occ = new OrderedConsumerConfiguration()
                .filterSubject(tsc.subject())
                .deliverPolicy(DeliverPolicy.ByStartSequence)
                .startSequence(2);
            occtx = sctx.createOrderedConsumer(occ);
            try (IterableConsumer consumer = occtx.iterate()) {
                Message m = consumer.nextMessage(1000);
                assertEquals(2, m.metaData().streamSequence());
            }
        });
    }

    @Test
    public void testOrderedIterableConsumerBasic() throws Exception {
        jsServer.run(TestBase::atLeast2_9_1, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            StreamContext sctx = nc.getStreamContext(tsc.stream);

            int stopCount = 500;
            OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration().filterSubject(tsc.subject());
            OrderedConsumerContext occtx = sctx.createOrderedConsumer(occ);
            assertNull(occtx.getConsumerName());
            try (IterableConsumer consumer = occtx.iterate()) {
                validateConsumerNameForOrdered(occtx, consumer, null);
                _testIterableBasic(js, stopCount, consumer, tsc.subject());
            }

            String consumerNamePrefix = prefix();
            occ = new OrderedConsumerConfiguration().filterSubject(tsc.subject()).consumerNamePrefix(consumerNamePrefix);
            occtx = sctx.createOrderedConsumer(occ);
            assertNull(occtx.getConsumerName());
            try (IterableConsumer consumer = occtx.iterate()) {
                validateConsumerNameForOrdered(occtx, consumer, consumerNamePrefix);
                _testIterableBasic(js, stopCount, consumer, tsc.subject());
            }
        });
    }

    private void _testIterableBasic(JetStream js, int stopCount, IterableConsumer consumer, String subject) throws InterruptedException {
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

        Publisher publisher = new Publisher(js, subject, 25);
        Thread pubThread = new Thread(publisher);
        pubThread.start();

        consumeThread.join();
        publisher.stop();
        pubThread.join();

        assertTrue(count.get() > 500);
    }

    @Test
    public void testConsumeWithHandler() throws Exception {
        jsServer.run(TestBase::atLeast2_9_1, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);

            JetStream js = nc.jetStream();
            jsPublish(js, tsc.subject(), 2500);

            // Pre define a consumer
            ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(tsc.consumerName()).build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);

            // Consumer[Context]
            ConsumerContext consumerContext = js.getConsumerContext(tsc.stream, tsc.consumerName());
            validateConsumerName(consumerContext, null, tsc.consumerName());

            int stopCount = 500;

            CountDownLatch latch = new CountDownLatch(1);
            AtomicInteger atomicCount = new AtomicInteger();
            MessageHandler handler = msg -> {
                msg.ack();
                if (atomicCount.incrementAndGet() == stopCount) {
                    latch.countDown();
                }
            };

            try (MessageConsumer mcon = consumerContext.consume(handler)) {
                validateConsumerName(consumerContext, mcon, tsc.consumerName());
                latch.await();
                stopAndWaitForFinished(mcon);
                assertTrue(atomicCount.get() > 500);
            }

            StreamContext sctx = nc.getStreamContext(tsc.stream);

            OrderedConsumerContext orderedConsumerContext =
                sctx.createOrderedConsumer(new OrderedConsumerConfiguration().filterSubject(tsc.subject()));
            assertNull(orderedConsumerContext.getConsumerName());

            CountDownLatch orderedLatch = new CountDownLatch(1);
            atomicCount.set(0);
            handler = msg -> {
                msg.ack();
                if (atomicCount.incrementAndGet() == stopCount) {
                    orderedLatch.countDown();
                }
            };

            try (MessageConsumer mcon = orderedConsumerContext.consume(handler)) {
                validateConsumerNameForOrdered(orderedConsumerContext, mcon, null);
                orderedLatch.await();
                stopAndWaitForFinished(mcon);
                assertTrue(atomicCount.get() > 500);
            }

            String prefix = prefix();
            OrderedConsumerContext orderedConsumerContextPrefixed =
                sctx.createOrderedConsumer(new OrderedConsumerConfiguration().filterSubject(tsc.subject()).consumerNamePrefix(prefix));
            assertNull(orderedConsumerContextPrefixed.getConsumerName());

            CountDownLatch orderedLatchPrefixed = new CountDownLatch(1);
            atomicCount.set(0);
            handler = msg -> {
                msg.ack();
                if (atomicCount.incrementAndGet() == stopCount) {
                    orderedLatchPrefixed.countDown();
                }
            };

            try (MessageConsumer mcon = orderedConsumerContextPrefixed.consume(handler)) {
                validateConsumerNameForOrdered(orderedConsumerContextPrefixed, mcon, prefix);
                orderedLatchPrefixed.await();
                stopAndWaitForFinished(mcon);
                assertTrue(atomicCount.get() > 500);
            }
        });
    }

    private static void stopAndWaitForFinished(MessageConsumer mcon) throws InterruptedException {
        mcon.stop();
        int fin = 0;
        while (!mcon.isFinished()) {
            //noinspection BusyWait
            Thread.sleep(10);
            if (++fin >= 500) {
                break;
            }
        }
    }

    @Test
    public void testNext() throws Exception {
        jsServer.run(TestBase::atLeast2_9_1, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            jsPublish(js, tsc.subject(), 4);

            String name = name();

            // Pre define a consumer
            ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(name).build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);

            // Consumer[Context]
            ConsumerContext consumerContext = js.getConsumerContext(tsc.stream, name);
            validateConsumerName(consumerContext, null, name);

            assertThrows(IllegalArgumentException.class, () -> consumerContext.next(1)); // max wait too small
            assertNotNull(consumerContext.next(1000));
            assertNotNull(consumerContext.next(Duration.ofMillis(1000)));
            assertNotNull(consumerContext.next(null));
            assertNotNull(consumerContext.next());
            assertNull(consumerContext.next(1000));

            StreamContext sctx = js.getStreamContext(tsc.stream);
            OrderedConsumerContext occtx = sctx.createOrderedConsumer(new OrderedConsumerConfiguration());
            assertNull(occtx.getConsumerName());
            assertThrows(IllegalArgumentException.class, () -> occtx.next(1)); // max wait too small

            assertNotNull(occtx.next(1000));
            String cname1 = validateConsumerNameForOrdered(occtx, null, null);

            assertNotNull(occtx.next(Duration.ofMillis(1000)));
            String cname2 = validateConsumerNameForOrdered(occtx, null, null);
            assertNotEquals(cname1, cname2);

            assertNotNull(occtx.next(null));
            cname1 = validateConsumerNameForOrdered(occtx, null, null);
            assertNotEquals(cname1, cname2);

            assertNotNull(occtx.next());
            cname2 = validateConsumerNameForOrdered(occtx, null, null);
            assertNotEquals(cname1, cname2);

            assertNull(occtx.next(1000));
            cname1 = validateConsumerNameForOrdered(occtx, null, null);
            assertNotEquals(cname1, cname2);

            String prefix = prefix();
            OrderedConsumerContext occtxPrefixed = sctx.createOrderedConsumer(new OrderedConsumerConfiguration().consumerNamePrefix(prefix));
            assertNull(occtxPrefixed.getConsumerName());
            assertThrows(IllegalArgumentException.class, () -> occtxPrefixed.next(1)); // max wait too small

            assertNotNull(occtxPrefixed.next(1000));
            cname1 = validateConsumerNameForOrdered(occtxPrefixed, null, prefix);

            assertNotNull(occtxPrefixed.next(Duration.ofMillis(1000)));
            cname2 = validateConsumerNameForOrdered(occtxPrefixed, null, prefix);
            assertNotEquals(cname1, cname2);

            assertNotNull(occtxPrefixed.next(null));
            cname1 = validateConsumerNameForOrdered(occtxPrefixed, null, prefix);
            assertNotEquals(cname1, cname2);

            assertNotNull(occtxPrefixed.next());
            cname2 = validateConsumerNameForOrdered(occtxPrefixed, null, prefix);
            assertNotEquals(cname1, cname2);

            assertNull(occtxPrefixed.next(1000));
            cname1 = validateConsumerNameForOrdered(occtxPrefixed, null, prefix);
            assertNotEquals(cname1, cname2);
        });
    }

    @Test
    public void testCoverage() throws Exception {
        jsServer.run(TestBase::atLeast2_9_1, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            JetStream js = nc.jetStream();

            // Pre define a consumer
            jsm.addOrUpdateConsumer(tsc.stream, ConsumerConfiguration.builder().durable(tsc.consumerName(1)).build());
            jsm.addOrUpdateConsumer(tsc.stream, ConsumerConfiguration.builder().durable(tsc.consumerName(2)).build());
            jsm.addOrUpdateConsumer(tsc.stream, ConsumerConfiguration.builder().durable(tsc.consumerName(3)).build());
            jsm.addOrUpdateConsumer(tsc.stream, ConsumerConfiguration.builder().durable(tsc.consumerName(4)).build());

            // Stream[Context]
            StreamContext sctx1 = nc.getStreamContext(tsc.stream);
            nc.getStreamContext(tsc.stream, JetStreamOptions.DEFAULT_JS_OPTIONS);
            js.getStreamContext(tsc.stream);

            // Consumer[Context]
            ConsumerContext cctx1 = nc.getConsumerContext(tsc.stream, tsc.consumerName(1));
            ConsumerContext cctx2 = nc.getConsumerContext(tsc.stream, tsc.consumerName(2), JetStreamOptions.DEFAULT_JS_OPTIONS);
            ConsumerContext cctx3 = js.getConsumerContext(tsc.stream, tsc.consumerName(3));
            ConsumerContext cctx4 = sctx1.getConsumerContext(tsc.consumerName(4));
            ConsumerContext cctx5 = sctx1.createOrUpdateConsumer(ConsumerConfiguration.builder().durable(tsc.consumerName(5)).build());
            ConsumerContext cctx6 = sctx1.createOrUpdateConsumer(ConsumerConfiguration.builder().durable(tsc.consumerName(6)).build());

            after(cctx1.iterate(), tsc.consumerName(1), true);
            after(cctx2.iterate(ConsumeOptions.DEFAULT_CONSUME_OPTIONS), tsc.consumerName(2), true);
            after(cctx3.consume(m -> {}), tsc.consumerName(3), true);
            after(cctx4.consume(ConsumeOptions.DEFAULT_CONSUME_OPTIONS, m -> {}), tsc.consumerName(4), true);
            after(cctx5.fetchMessages(1), tsc.consumerName(5), false);
            after(cctx6.fetchBytes(1000), tsc.consumerName(6), false);
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
    public void testFetchConsumeOptionsBuilder() throws IOException, ClassNotFoundException {
        FetchConsumeOptions fco = FetchConsumeOptions.builder().build();
        check_default_values(fco);
        check_default_values(roundTripSerialize(fco));

        // COVERAGE
        SerializableFetchConsumeOptions sfco = new SerializableFetchConsumeOptions();
        sfco.setFetchConsumeOptions(fco);
        check_default_values(sfco.getFetchConsumeOptions());

        // COVERAGE
        sfco = new SerializableFetchConsumeOptions(FetchConsumeOptions.builder());
        sfco.setFetchConsumeOptions(fco);
        check_default_values(sfco.getFetchConsumeOptions());

        fco = FetchConsumeOptions.builder().maxMessages(1000).build();
        check_values(fco, 1000, 0, DEFAULT_THRESHOLD_PERCENT);
        check_values(roundTripSerialize(fco), 1000, 0, DEFAULT_THRESHOLD_PERCENT);

        fco = FetchConsumeOptions.builder().maxMessages(1000).thresholdPercent(50).build();
        check_values(fco, 1000, 0, 50);
        check_values(roundTripSerialize(fco), 1000, 0, 50);

        fco = FetchConsumeOptions.builder().max(1000, 100).build();
        check_values(fco, 100, 1000, DEFAULT_THRESHOLD_PERCENT);
        check_values(roundTripSerialize(fco), 100, 1000, DEFAULT_THRESHOLD_PERCENT);

        fco = FetchConsumeOptions.builder().max(1000, 100).thresholdPercent(50).build();
        check_values(fco, 100, 1000, 50);
        check_values(roundTripSerialize(fco), 100, 1000, 50);

        fco = FetchConsumeOptions.builder().group("g").minPending(1).minAckPending(2).build();
        assertEquals("g", fco.getGroup());
        assertEquals(1, fco.getMinPending());
        assertEquals(2, fco.getMinAckPending());

        fco = roundTripSerialize(fco);
        assertEquals("g", fco.getGroup());
        assertEquals(1, fco.getMinPending());
        assertEquals(2, fco.getMinAckPending());
    }

    private void check_default_values(FetchConsumeOptions fco) {
        assertEquals(DEFAULT_MESSAGE_COUNT, fco.getMaxMessages());
        assertEquals(DEFAULT_EXPIRES_IN_MILLIS, fco.getExpiresInMillis());
        assertEquals(DEFAULT_THRESHOLD_PERCENT, fco.getThresholdPercent());
        assertEquals(0, fco.getMaxBytes());
        assertEquals(DEFAULT_EXPIRES_IN_MILLIS * MAX_IDLE_HEARTBEAT_PERCENT / 100, fco.getIdleHeartbeat());
    }

    private void check_values(FetchConsumeOptions fco, int maxMessages, int maxBytes, int thresholdPercent) throws IOException, ClassNotFoundException {
        _check_values(fco, maxMessages, maxBytes, thresholdPercent);
        fco = roundTripSerialize(fco);
        _check_values(fco, maxMessages, maxBytes, thresholdPercent);
    }

    private void _check_values(FetchConsumeOptions fco, int maxMessages, int maxBytes, int thresholdPercent) {
        assertEquals(maxMessages, fco.getMaxMessages());
        assertEquals(maxBytes, fco.getMaxBytes());
        assertEquals(thresholdPercent, fco.getThresholdPercent());
        assertNull(fco.getGroup());
        assertEquals(-1, fco.getMinPending());
        assertEquals(-1, fco.getMinAckPending());
    }

    private FetchConsumeOptions roundTripSerialize(FetchConsumeOptions fco) throws IOException, ClassNotFoundException {
        SerializableFetchConsumeOptions sfco = new SerializableFetchConsumeOptions(fco);
        sfco = (SerializableFetchConsumeOptions)roundTripSerialize(sfco);
        return sfco.getFetchConsumeOptions();
    }

    @Test
    public void testConsumeOptionsBuilder() throws IOException, ClassNotFoundException {
        ConsumeOptions co = ConsumeOptions.builder().build();
        check_default_values(co);
        check_default_values(roundTripSerialize(co));

        co = ConsumeOptions.builder().batchSize(1000).build();
        check_values(co, 1000, 0, DEFAULT_THRESHOLD_PERCENT);
        check_values(roundTripSerialize(co), 1000, 0, DEFAULT_THRESHOLD_PERCENT);

        co = ConsumeOptions.builder().batchSize(1000).thresholdPercent(50).build();
        check_values(co, 1000, 0, 50);
        check_values(roundTripSerialize(co), 1000, 0, 50);

        co = ConsumeOptions.builder().batchBytes(1000).build();
        check_values(co, DEFAULT_MESSAGE_COUNT_WHEN_BYTES, 1000, DEFAULT_THRESHOLD_PERCENT);
        check_values(roundTripSerialize(co), DEFAULT_MESSAGE_COUNT_WHEN_BYTES, 1000, DEFAULT_THRESHOLD_PERCENT);

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

        co = ConsumeOptions.builder().group("g").minPending(1).minAckPending(2).build();
        assertEquals("g", co.getGroup());
        assertEquals(1, co.getMinPending());
        assertEquals(2, co.getMinAckPending());

        co = roundTripSerialize(co);
        assertEquals("g", co.getGroup());
        assertEquals(1, co.getMinPending());
        assertEquals(2, co.getMinAckPending());
    }

    private void check_default_values(ConsumeOptions co) {
        assertEquals(DEFAULT_MESSAGE_COUNT, co.getBatchSize());
        assertEquals(DEFAULT_EXPIRES_IN_MILLIS, co.getExpiresInMillis());
        assertEquals(DEFAULT_THRESHOLD_PERCENT, co.getThresholdPercent());
        assertEquals(0, co.getBatchBytes());
        assertEquals(DEFAULT_EXPIRES_IN_MILLIS * MAX_IDLE_HEARTBEAT_PERCENT / 100, co.getIdleHeartbeat());
    }

    private void check_values(ConsumeOptions co, int batchSize, int batchBytes, int thresholdPercent) {
        assertEquals(batchSize, co.getBatchSize());
        assertEquals(batchBytes, co.getBatchBytes());
        assertEquals(thresholdPercent, co.getThresholdPercent());
        assertNull(co.getGroup());
        assertEquals(-1, co.getMinPending());
        assertEquals(-1, co.getMinAckPending());
    }

    private ConsumeOptions roundTripSerialize(ConsumeOptions co) throws IOException, ClassNotFoundException {
        SerializableConsumeOptions sco = new SerializableConsumeOptions(co);
        sco = (SerializableConsumeOptions)roundTripSerialize(sco);
        return sco.getConsumeOptions();
    }

    // this sim is different from the other sim b/c next has a new sub every message
    public static class PullOrderedNextTestDropSimulator extends PullOrderedMessageManager {
        @SuppressWarnings("ClassEscapesDefinedScope")
        public PullOrderedNextTestDropSimulator(NatsConnection conn, NatsJetStream js, String stream, SubscribeOptions so, ConsumerConfiguration serverCC, boolean queueMode, boolean syncMode) {
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
    public void testOrderedBehaviorNext() throws Exception {
        jsServer.run(TestBase::atLeast2_9_1, nc -> {
            // Setup
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            StreamContext sctx = js.getStreamContext(tsc.stream);

            jsPublish(js, tsc.subject(), 101, 6, 100);
            ZonedDateTime startTime = getStartTimeFirstMessage(js, tsc);

            // New pomm factory in place before each subscription is made
            // test with and without a consumer name prefix

            ((NatsJetStream)js)._pullOrderedMessageManagerFactory = PullOrderedNextTestDropSimulator::new;
            _testOrderedNext(sctx, 1, new OrderedConsumerConfiguration()
                .filterSubject(tsc.subject()));
            _testOrderedNext(sctx, 1, new OrderedConsumerConfiguration()
                .consumerNamePrefix(prefix())
                .filterSubject(tsc.subject()));

            ((NatsJetStream)js)._pullOrderedMessageManagerFactory = PullOrderedNextTestDropSimulator::new;
            _testOrderedNext(sctx, 2, new OrderedConsumerConfiguration().filterSubject(tsc.subject())
                .deliverPolicy(DeliverPolicy.ByStartTime).startTime(startTime));
            _testOrderedNext(sctx, 2, new OrderedConsumerConfiguration().filterSubject(tsc.subject())
                .consumerNamePrefix(prefix())
                .deliverPolicy(DeliverPolicy.ByStartTime).startTime(startTime));

            ((NatsJetStream)js)._pullOrderedMessageManagerFactory = PullOrderedNextTestDropSimulator::new;
            _testOrderedNext(sctx, 2, new OrderedConsumerConfiguration().filterSubject(tsc.subject())
                .deliverPolicy(DeliverPolicy.ByStartSequence).startSequence(2));
            _testOrderedNext(sctx, 2, new OrderedConsumerConfiguration().filterSubject(tsc.subject())
                .consumerNamePrefix(prefix())
                .deliverPolicy(DeliverPolicy.ByStartSequence).startSequence(2));
        });
    }

    private ZonedDateTime getStartTimeFirstMessage(JetStream js, TestingStreamContainer tsc) throws IOException, JetStreamApiException, InterruptedException {
        ZonedDateTime startTime;
        JetStreamSubscription sub = js.subscribe(tsc.subject());
        Message mt = sub.nextMessage(1000);
        startTime = mt.metaData().timestamp().plus(30, ChronoUnit.MILLIS);
        sub.unsubscribe();
        return startTime;
    }

    private void _testOrderedNext(StreamContext sctx, int expectedStreamSeq, OrderedConsumerConfiguration occ) throws IOException, JetStreamApiException, InterruptedException, JetStreamStatusCheckedException {
        OrderedConsumerContext occtx = sctx.createOrderedConsumer(occ);
        assertNull(occtx.getConsumerName());
        // Loop through the messages to make sure I get stream sequence 1 to 6
        while (expectedStreamSeq <= 6) {
            Message m = occtx.next(1000);
            if (m != null) {
                if (occ.getConsumerNamePrefix() != null) {
                    assertTrue(occtx.getConsumerName().startsWith(occ.getConsumerNamePrefix()));
                }
                assertEquals(expectedStreamSeq, m.metaData().streamSequence());
                assertEquals(1, m.metaData().consumerSequence());
                ++expectedStreamSeq;
            }
        }
        validateConsumerNameForOrdered(occtx, null, occ.getConsumerNamePrefix());
    }

    public static long CS_FOR_SS_3 = 3;
    public static class PullOrderedTestDropSimulator extends PullOrderedMessageManager {
        @SuppressWarnings("ClassEscapesDefinedScope")
        public PullOrderedTestDropSimulator(NatsConnection conn, NatsJetStream js, String stream, SubscribeOptions so, ConsumerConfiguration serverCC, boolean queueMode, boolean syncMode) {
            super(conn, js, stream, so, serverCC, syncMode);
        }

        @Override
        protected Boolean beforeQueueProcessorImpl(NatsMessage msg) {
            if (msg.isJetStream()
                && msg.metaData().streamSequence() == 3
                && msg.metaData().consumerSequence() == CS_FOR_SS_3)
            {
                return false;
            }

            return super.beforeQueueProcessorImpl(msg);
        }
    }

    @Test
    public void testOrderedBehaviorFetch() throws Exception {
        jsServer.run(TestBase::atLeast2_9_1, nc -> {
            // Setup
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            StreamContext sctx = js.getStreamContext(tsc.stream);

            jsPublish(js, tsc.subject(), 101, 6, 100);
            ZonedDateTime startTime = getStartTimeFirstMessage(js, tsc);

            // New pomm factory in place before subscriptions are made
            ((NatsJetStream)js)._pullOrderedMessageManagerFactory = PullOrderedTestDropSimulator::new;

            // Set the Consumer Sequence For Stream Sequence 3 statically for ease
            CS_FOR_SS_3 = 3;
            _testOrderedFetch(sctx, 1, new OrderedConsumerConfiguration().filterSubject(tsc.subject()));
            _testOrderedFetch(sctx, 1, new OrderedConsumerConfiguration()
                .consumerNamePrefix(prefix())
                .filterSubject(tsc.subject()));

            CS_FOR_SS_3 = 2;
            _testOrderedFetch(sctx, 2, new OrderedConsumerConfiguration().filterSubject(tsc.subject())
                .deliverPolicy(DeliverPolicy.ByStartTime).startTime(startTime));
            _testOrderedFetch(sctx, 2, new OrderedConsumerConfiguration().filterSubject(tsc.subject())
                .consumerNamePrefix(prefix())
                .deliverPolicy(DeliverPolicy.ByStartTime).startTime(startTime));

            CS_FOR_SS_3 = 2;
            _testOrderedFetch(sctx, 2, new OrderedConsumerConfiguration().filterSubject(tsc.subject())
                .deliverPolicy(DeliverPolicy.ByStartSequence).startSequence(2));
            _testOrderedFetch(sctx, 2, new OrderedConsumerConfiguration().filterSubject(tsc.subject())
                .consumerNamePrefix(prefix())
                .deliverPolicy(DeliverPolicy.ByStartSequence).startSequence(2));
        });
    }

    private void _testOrderedFetch(StreamContext sctx, int expectedStreamSeq, OrderedConsumerConfiguration occ) throws Exception {
        OrderedConsumerContext occtx = sctx.createOrderedConsumer(occ);
        assertNull(occtx.getConsumerName());
        FetchConsumeOptions fco = FetchConsumeOptions.builder().maxMessages(6).expiresIn(1000).build();
        String firstConsumerName;
        try (FetchConsumer fcon = occtx.fetch(fco)) {
            firstConsumerName = validateConsumerNameForOrdered(occtx, null, occ.getConsumerNamePrefix());
            Message m = fcon.nextMessage();
            while (m != null) {
                assertEquals(expectedStreamSeq++, m.metaData().streamSequence());
                m = fcon.nextMessage();
            }
            // we know this because the simulator is designed to fail the first time at the third message
            assertEquals(3, expectedStreamSeq);
            // fetch failure will stop the consumer, but make sure it's done b/c with ordered
            // I can't have more than one consuming at a time.
            while (!fcon.isFinished()) {
                sleep(1);
            }
        }
        // this should finish without error
        try (FetchConsumer fcon = occtx.fetch(fco)) {
            validateConsumerNameForOrdered(occtx, null, occ.getConsumerNamePrefix());
            assertNotEquals(firstConsumerName, occtx.getConsumerName());
            Message m = fcon.nextMessage();
            while (expectedStreamSeq <= 6) {
                assertEquals(expectedStreamSeq++, m.metaData().streamSequence());
                m = fcon.nextMessage();
            }
        }
    }

    @Test
    public void testOrderedBehaviorIterable() throws Exception {
        jsServer.run(TestBase::atLeast2_9_1, nc -> {
            jsServer.setExitOnDisconnect();
            jsServer.setExitOnHeartbeatError();

            // Setup
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            StreamContext sctx = js.getStreamContext(tsc.stream);

            jsPublish(js, tsc.subject(), 101, 6, 100);
            ZonedDateTime startTime = getStartTimeFirstMessage(js, tsc);

            // New pomm factory in place before each subscription is made
            // Set the Consumer Sequence For Stream Sequence 3 statically for ease
            CS_FOR_SS_3 = 3;
            ((NatsJetStream)js)._pullOrderedMessageManagerFactory = PullOrderedTestDropSimulator::new;
            _testOrderedIterate(sctx, 1, new OrderedConsumerConfiguration().filterSubject(tsc.subject()));
            _testOrderedIterate(sctx, 1, new OrderedConsumerConfiguration()
                .consumerNamePrefix(prefix())
                .filterSubject(tsc.subject()));

            CS_FOR_SS_3 = 2;
            ((NatsJetStream)js)._pullOrderedMessageManagerFactory = PullOrderedTestDropSimulator::new;
            _testOrderedIterate(sctx, 2, new OrderedConsumerConfiguration().filterSubject(tsc.subject())
                .deliverPolicy(DeliverPolicy.ByStartTime).startTime(startTime));
            _testOrderedIterate(sctx, 2, new OrderedConsumerConfiguration().filterSubject(tsc.subject())
                .consumerNamePrefix(prefix())
                .deliverPolicy(DeliverPolicy.ByStartTime).startTime(startTime));

            CS_FOR_SS_3 = 2;
            ((NatsJetStream)js)._pullOrderedMessageManagerFactory = PullOrderedTestDropSimulator::new;
            _testOrderedIterate(sctx, 2, new OrderedConsumerConfiguration().filterSubject(tsc.subject())
                .deliverPolicy(DeliverPolicy.ByStartSequence).startSequence(2));
            _testOrderedIterate(sctx, 2, new OrderedConsumerConfiguration().filterSubject(tsc.subject())
                .consumerNamePrefix(prefix())
                .deliverPolicy(DeliverPolicy.ByStartSequence).startSequence(2));
        });
    }

    private void _testOrderedIterate(StreamContext sctx, int expectedStreamSeq, OrderedConsumerConfiguration occ) throws Exception {
        OrderedConsumerContext occtx = sctx.createOrderedConsumer(occ);
        assertNull(occtx.getConsumerName());
        try (IterableConsumer icon = occtx.iterate()) {
            validateConsumerNameForOrdered(occtx, icon, occ.getConsumerNamePrefix());
            // Loop through the messages to make sure I get stream sequence 1 to 5
            while (expectedStreamSeq <= 5) {
                Message m = icon.nextMessage(Duration.ofSeconds(1)); // use the duration version here for coverage
                if (m != null) {
                    assertEquals(expectedStreamSeq++, m.metaData().streamSequence());
                }
            }
        }
    }

    @Test
    public void testOrderedConsume() throws Exception {
        jsServer.run(TestBase::atLeast2_9_1, nc -> {
            // Setup
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration().filterSubject(tsc.subject());
            _testOrderedConsume(js, tsc, occ);

            tsc = new TestingStreamContainer(jsm);
            occ = new OrderedConsumerConfiguration()
                .consumerNamePrefix(prefix())
                .filterSubject(tsc.subject());
            _testOrderedConsume(js, tsc, occ);
        });
    }

    private void _testOrderedConsume(JetStream js, TestingStreamContainer tsc, OrderedConsumerConfiguration occ) throws Exception {
        StreamContext sctx = js.getStreamContext(tsc.stream);

        // Get this in place before subscriptions are made
        ((NatsJetStream) js)._pullOrderedMessageManagerFactory = PullOrderedTestDropSimulator::new;

        CountDownLatch msgLatch = new CountDownLatch(6);
        AtomicInteger received = new AtomicInteger();
        AtomicLong[] ssFlags = new AtomicLong[6];
        MessageHandler handler = hmsg -> {
            int i = received.incrementAndGet() - 1;
            ssFlags[i] = new AtomicLong(hmsg.metaData().streamSequence());
            msgLatch.countDown();
        };
        OrderedConsumerContext occtx = sctx.createOrderedConsumer(occ);
        assertNull(occtx.getConsumerName());
        try (MessageConsumer mcon = occtx.consume(handler)) {
            validateConsumerNameForOrdered(occtx, mcon, occ.getConsumerNamePrefix());
            jsPublish(js, tsc.subject(), 201, 6);

            // wait for the messages
            awaitAndAssert(msgLatch);

            // Loop through the messages to make sure I get stream sequence 1 to 6
            int expectedStreamSeq = 1;
            while (expectedStreamSeq <= 6) {
                int idx = expectedStreamSeq - 1;
                assertEquals(expectedStreamSeq++, ssFlags[idx].get());
            }
        }
    }

    @Test
    public void testOrderedConsumeMultipleSubjects() throws Exception {
        jsServer.run(TestBase::atLeast2_10, nc -> {
            // Setup
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            TestingStreamContainer tsc = new TestingStreamContainer(jsm, 2);
            jsPublish(js, tsc.subject(0), 10);
            jsPublish(js, tsc.subject(1), 5);

            StreamContext sctx = js.getStreamContext(tsc.stream);

            OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration().filterSubjects(tsc.subject(0), tsc.subject(1));
            OrderedConsumerContext occtx = sctx.createOrderedConsumer(occ);

            int count0 = 0;
            int count1 = 0;
            try (FetchConsumer fc = occtx.fetch(FetchConsumeOptions.builder().maxMessages(20).expiresIn(2000).build())) {
                Message m = fc.nextMessage();
                while (m != null) {
                    if (m.getSubject().equals(tsc.subject(0))) {
                        count0++;
                    }
                    else {
                        count1++;
                    }
                    m.ack();
                    m = fc.nextMessage();
                }
            }

            assertEquals(10, count0);
            assertEquals(5, count1);
        });
    }

    @Test
    public void testOrderedMultipleWays() throws Exception {
        jsServer.run(TestBase::atLeast2_9_1, nc -> {
            // Setup
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            createMemoryStream(jsm, tsc.stream, tsc.subject());

            StreamContext sctx = js.getStreamContext(tsc.stream);

            OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration().filterSubject(tsc.subject());
            OrderedConsumerContext occtx = sctx.createOrderedConsumer(occ);

            // can't do others while doing next
            CountDownLatch latch = new CountDownLatch(1);
            new Thread(() -> {
                try {
                    // make sure there is enough time to call other methods.
                    assertNull(occtx.next(2000));
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
                finally {
                    latch.countDown();
                }
            }).start();

            Thread.sleep(100); // make sure there is enough time for the thread to start and get into the next method
            validateCantCallOtherMethods(occtx);

            //noinspection ResultOfMethodCallIgnored
            latch.await(3000, TimeUnit.MILLISECONDS);

            for (int x = 0 ; x < 10_000; x++) {
                js.publish(tsc.subject(), ("multiple" + x).getBytes());
            }

            // can do others now
            Message m = occtx.next(1000);
            assertNotNull(m);
            assertEquals(1, m.metaData().streamSequence());

            // can't do others while doing next
            int seq = 2;
            try (FetchConsumer fc = occtx.fetchMessages(5)) {
                while (seq <= 6) {
                    m = fc.nextMessage();
                    assertNotNull(m);
                    assertEquals(seq, m.metaData().streamSequence());
                    assertFalse(fc.isFinished());
                    validateCantCallOtherMethods(occtx);
                    seq++;
                }

                assertNull(fc.nextMessage());
                assertTrue(fc.isFinished());
                assertNull(fc.nextMessage()); // just some coverage
            }

            // can do others now
            m = occtx.next(1000);
            assertNotNull(m);
            assertEquals(seq++, m.metaData().streamSequence());

            // can't do others while doing iterate
            ConsumeOptions copts = ConsumeOptions.builder().batchSize(10).build();
            try (IterableConsumer ic = occtx.iterate(copts)) {
                ic.stop();
                m = ic.nextMessage(1000);
                while (m != null) {
                    assertEquals(seq, m.metaData().streamSequence());
                    if (!ic.isFinished()) {
                        validateCantCallOtherMethods(occtx);
                    }
                    ++seq;
                    m = ic.nextMessage(1000);
                }
            }

            // can do others now
            m = occtx.next(1000);
            assertNotNull(m);
            assertEquals(seq++, m.metaData().streamSequence());

            int last = Math.min(seq + 10, 9999);
            try (FetchConsumer fc = occtx.fetchMessages(last - seq)) {
                while (seq < last) {
                    fc.stop();
                    m = fc.nextMessage();
                    assertNotNull(m);
                    assertEquals(seq, m.metaData().streamSequence());
                    assertFalse(fc.isFinished());
                    validateCantCallOtherMethods(occtx);
                    seq++;
                }
            }
        });
    }

    @SuppressWarnings("resource")
    private void validateCantCallOtherMethods(OrderedConsumerContext ctx) {
        assertThrows(IOException.class, () -> ctx.next(1000));
        assertThrows(IOException.class, () -> ctx.fetchMessages(1));
        assertThrows(IllegalArgumentException.class, () -> ctx.consume(null));
    }

    @Test
    public void testOrderedConsumerBuilder() throws IOException, ClassNotFoundException {
        OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration();
        check_default_values(occ);
        check_default_values(roundTripSerialize(occ));

        // COVERAGE
        check_default_values(new SerializableOrderedConsumerConfiguration().getOrderedConsumerConfiguration());

        // COVERAGE
        SerializableOrderedConsumerConfiguration socc = new SerializableOrderedConsumerConfiguration();
        socc.setOrderedConsumerConfiguration(occ);
        check_default_values(socc.getOrderedConsumerConfiguration());

        // nulls
        occ = new OrderedConsumerConfiguration()
            .filterSubject(null)
            .deliverPolicy(null)
            .replayPolicy(null)
            .headersOnly(null);
        check_default_values(occ);
        check_default_values(roundTripSerialize(occ));

        // values that set to default
        occ = new OrderedConsumerConfiguration()
            .filterSubject("")
            .startSequence(-42)
            .headersOnly(false);
        check_default_values(occ);
        check_default_values(roundTripSerialize(occ));

        // values
        ZonedDateTime zdt = DateTimeUtils.toGmt(ZonedDateTime.now());
        occ = new OrderedConsumerConfiguration()
            .filterSubject("fs")
            .deliverPolicy(DeliverPolicy.All)
            .startSequence(42)
            .startTime(zdt)
            .replayPolicy(ReplayPolicy.Original)
            .headersOnly(true);
        check_values(occ, zdt);
        check_values(roundTripSerialize(occ), zdt);

        // COVERAGE - depends on state from last occ
        socc = new SerializableOrderedConsumerConfiguration();
        socc.setOrderedConsumerConfiguration(occ);
        occ = socc.getOrderedConsumerConfiguration();
        check_values(occ, zdt);
        check_values(roundTripSerialize(occ), zdt);

        // Multiple and COVERAGE
        occ = new OrderedConsumerConfiguration().filterSubjects("fs0", "fs1");
        assertNull(occ.getFilterSubject());
        assertTrue(occ.hasMultipleFilterSubjects());
        assertNotNull(occ.getFilterSubjects());
        assertEquals("fs0", occ.getFilterSubjects().get(0));
        assertEquals("fs1", occ.getFilterSubjects().get(1));
        occ = new OrderedConsumerConfiguration(JsonParser.parse(occ.toJson()));
        assertNull(occ.getFilterSubject());
        assertTrue(occ.hasMultipleFilterSubjects());
        assertNotNull(occ.getFilterSubjects());
        assertEquals("fs0", occ.getFilterSubjects().get(0));
        assertEquals("fs1", occ.getFilterSubjects().get(1));
    }

    private void check_default_values(OrderedConsumerConfiguration occ) {
        assertEquals(">", occ.getFilterSubject());
        assertNotNull(occ.getFilterSubject());
        assertFalse(occ.hasMultipleFilterSubjects());
        assertNull(occ.getDeliverPolicy());
        assertEquals(ConsumerConfiguration.LONG_UNSET, occ.getStartSequence());
        assertNull(occ.getStartTime());
        assertNull(occ.getReplayPolicy());
        assertNull(occ.getHeadersOnly());
    }

    private void check_values(OrderedConsumerConfiguration occ, ZonedDateTime zdt) {
        assertEquals("fs", occ.getFilterSubject());
        assertEquals(DeliverPolicy.All, occ.getDeliverPolicy());
        assertEquals(42, occ.getStartSequence());
        assertEquals(zdt, occ.getStartTime());
        assertEquals(ReplayPolicy.Original, occ.getReplayPolicy());
        assertTrue(occ.getHeadersOnly());
    }

    private OrderedConsumerConfiguration roundTripSerialize(OrderedConsumerConfiguration occ) throws IOException, ClassNotFoundException {
        SerializableOrderedConsumerConfiguration socc = new SerializableOrderedConsumerConfiguration(occ);
        socc = (SerializableOrderedConsumerConfiguration)roundTripSerialize(socc);
        return socc.getOrderedConsumerConfiguration();
    }

    private Object roundTripSerialize(Serializable s) throws IOException, ClassNotFoundException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(s);
            oos.flush();
            return new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray())).readObject();
        }
    }

    @Test
    public void testOverflowFetch() throws Exception {
        ListenerForTesting l = new ListenerForTesting();
        Options.Builder b = Options.builder().errorListener(l);
        jsServer.run(b, TestBase::atLeast2_11, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            JetStream js = nc.jetStream();
            jsPublish(js, tsc.subject(), 100);

            // Testing min ack pending
            String group = variant();
            String cname = variant();

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .name(cname)
                .priorityPolicy(PriorityPolicy.Overflow)
                .priorityGroups(group)
                .ackWait(10_000)
                .filterSubjects(tsc.subject()).build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);

            ConsumerContext ctxPrime = nc.getConsumerContext(tsc.stream, cname);
            ConsumerContext ctxOver = nc.getConsumerContext(tsc.stream, cname);

            FetchConsumeOptions fcoNoMin = FetchConsumeOptions.builder()
                .maxMessages(5).expiresIn(1000).group(group)
                .build();

            FetchConsumeOptions fcoOverA = FetchConsumeOptions.builder()
                .maxMessages(5).expiresIn(1000).group(group).minAckPending(5)
                .build();

            FetchConsumeOptions fcoOverB = FetchConsumeOptions.builder()
                .maxMessages(5).expiresIn(1000).group(group).minAckPending(10)
                .build();

            _overflowFetch(cname, ctxPrime, fcoNoMin, true, 5, 0);
            _overflowFetch(cname, ctxOver, fcoNoMin, true, 5, 0);

            _overflowFetch(cname, ctxPrime, fcoNoMin, false, 5, 5);
            _overflowFetch(cname, ctxOver, fcoOverA, true, 5, 5);
            _overflowFetch(cname, ctxOver, fcoOverB, true, 0, 5);
        });
    }

    private void _overflowFetch(String cname, ConsumerContext cctx, FetchConsumeOptions fco, boolean ack, int expected, int ackPendingWhenDone) throws Exception {
        try (FetchConsumer fc = cctx.fetch(fco)) {
            validateConsumerName(cctx, fc, cname);
            int count = 0;
            Message m = fc.nextMessage();
            while (m != null) {
                count++;
                if (ack) {
                    m.ack();
                }
                m = fc.nextMessage();
            }
            assertEquals(expected, count);
            if (ack) {
                sleep(50); // give the server time to process acks given
            }
            assertEquals(ackPendingWhenDone, cctx.getConsumerInfo().getNumAckPending());
        }
    }

    @Test
    public void testOverflowIterate() throws Exception {
        ListenerForTesting l = new ListenerForTesting();
        Options.Builder b = Options.builder().errorListener(l);
        runInJsServer(b, TestBase::atLeast2_11, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            JetStream js = nc.jetStream();
            jsPublish(js, tsc.subject(), 100);

            // Testing min ack pending
            String group = variant();
            String cname = variant();

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .name(cname)
                .priorityPolicy(PriorityPolicy.Overflow)
                .priorityGroups(group)
                .ackWait(30_000)
                .filterSubjects(tsc.subject()).build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);

            ConsumerContext ctxPrime = nc.getConsumerContext(tsc.stream, cname);
            ConsumerContext ctxOver = nc.getConsumerContext(tsc.stream, cname);
            validateConsumerName(ctxPrime, null, cname);
            validateConsumerName(ctxOver, null, cname);

            ConsumeOptions coPrime = ConsumeOptions.builder()
                .group(group)
                .build();

            ConsumeOptions coOver = ConsumeOptions.builder()
                .group(group)
                .minAckPending(101)
                .build();

            // start the overflow consumer
            AtomicLong primeCount = new AtomicLong();
            AtomicLong overCount = new AtomicLong();
            AtomicLong left = new AtomicLong(100);

            Thread tOver = new Thread(() -> {
                try {
                    IterableConsumer ic = ctxOver.iterate(coOver);
                    validateConsumerName(ctxOver, ic, cname);
                    while (left.get() > 0 && !Thread.currentThread().isInterrupted()) {
                        Message m = ic.nextMessage(100);
                        if (m != null) {
                            m.ack();
                            overCount.incrementAndGet();
                            left.decrementAndGet();
                        }
                    }
                }
                catch (InterruptedException ignore) {
                }
                catch (Exception e) {
                    fail(e);
                }
            });
            tOver.start();

            Thread tPrime = new Thread(() -> {
                try {
                    IterableConsumer ic = ctxPrime.iterate(coPrime);
                    validateConsumerName(ctxPrime, ic, cname);
                    while (left.get() > 0 && !Thread.currentThread().isInterrupted()) {
                        Message m = ic.nextMessage(100);
                        if (m != null) {
                            m.ack();
                            primeCount.incrementAndGet();
                            left.decrementAndGet();
                        }
                    }
                }
                catch (InterruptedException ignore) {
                }
                catch (Exception e) {
                    fail(e);
                }
            });
            tPrime.start();

            tPrime.join();
            tOver.join();
            assertEquals(100, primeCount.get());
            assertEquals(0, overCount.get());
        });
    }

    @Test
    public void testOverflowConsume() throws Exception {
        ListenerForTesting l = new ListenerForTesting();
        Options.Builder b = Options.builder().errorListener(l);
        runInJsServer(b, TestBase::atLeast2_11, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            JetStream js = nc.jetStream();
            jsPublish(js, tsc.subject(), 1000);

            // Testing min ack pending
            String group = variant();
            String cname = variant();

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .name(cname)
                .priorityPolicy(PriorityPolicy.Overflow)
                .priorityGroups(group)
                .ackWait(30_000)
                .filterSubjects(tsc.subject()).build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);

            ConsumerContext ctxPrime = nc.getConsumerContext(tsc.stream, cname);
            ConsumerContext ctxOver = nc.getConsumerContext(tsc.stream, cname);
            validateConsumerName(ctxPrime, null, cname);
            validateConsumerName(ctxOver, null, cname);

            ConsumeOptions coPrime = ConsumeOptions.builder()
                .group(group)
                .build();

            ConsumeOptions coOver = ConsumeOptions.builder()
                .group(group)
                .minAckPending(1001)
                .build();

            // start the overflow consumer
            AtomicLong primeCount = new AtomicLong();
            AtomicLong overCount = new AtomicLong();
            AtomicLong left = new AtomicLong(500);

            MessageHandler overHandler = m -> {
                m.ack();
                overCount.incrementAndGet();
                left.decrementAndGet();
            };

            MessageHandler primeHandler = m -> {
                m.ack();
                primeCount.incrementAndGet();
                left.decrementAndGet();
            };

            try (MessageConsumer mcOver = ctxOver.consume(coOver, overHandler);
                 MessageConsumer mcPrime = ctxPrime.consume(coPrime, primeHandler))
            {
                validateConsumerName(ctxPrime, mcPrime, cname);
                validateConsumerName(ctxOver, mcOver, cname);
                while (left.get() > 0) {
                    sleep(100);
                }
                mcOver.stop();
                mcPrime.stop();
            }

            assertTrue(primeCount.get() > 0);
            assertEquals(0, overCount.get());
        });
    }

    @Test
    public void testFinishEmptyStream() throws Exception {
        ListenerForTesting l = new ListenerForTesting();
        Options.Builder b = Options.builder().errorListener(l);
        runInJsServer(b, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            TestingStreamContainer tsc = new TestingStreamContainer(jsm);

            String name = variant();

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .name(name)
                .filterSubjects(tsc.subject()).build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);

            ConsumerContext cctx = nc.getConsumerContext(tsc.stream, name);

            MessageHandler handler = Message::ack;

            ConsumeOptions co = ConsumeOptions.builder().expiresIn(1000).build();
            try (MessageConsumer consumer = cctx.consume(co, handler)) {
                consumer.stop();
                sleep(1200); // more than the expires period for the consume
                assertTrue(consumer.isFinished());
            }
        });
    }

    @Test
    public void testReconnectOverOrdered() throws Exception {
        // ------------------------------------------------------------
        // The idea here is...
        // 1. connect with an ordered consumer and start consuming
        // 2. stop the server then restart it causing a disconnect,
        //    but reconnect before the idle heartbeat alarm kicks in
        // 3. stop the server but wait a little before restarting
        //    so the alarm goes off but still disconnected
        //    to make sure the consumer continues after that condition
        // ------------------------------------------------------------
        int port = NatsTestServer.nextPort();
        ListenerForTesting lft = new ListenerForTesting();
        Options options = new Options.Builder()
            .connectionListener(lft)
            .errorListener(lft)
            .server(NatsTestServer.getNatsLocalhostUri(port)).build();
        NatsConnection nc;

        String stream = stream();
        String subject = subject();

        AtomicBoolean allInOrder = new AtomicBoolean(true);
        AtomicInteger atomicCount = new AtomicInteger();
        AtomicLong nextExpectedSequence = new AtomicLong(0);

        MessageHandler handler = msg -> {
            if (msg.metaData().streamSequence() != nextExpectedSequence.incrementAndGet()) {
                allInOrder.set(false);
            }
            msg.ack();
            atomicCount.incrementAndGet();
            sleep(50); // simulate some work and to slow the endless consume
        };

        // variable are here. initialized during first server, but used after.
        StreamContext streamContext;
        OrderedConsumerContext orderedConsumerContext;
        MessageConsumer mcon;
        String firstConsumerName;

        //noinspection unused
        try (NatsTestServer ts = new NatsTestServer(port, false, true)) {
            nc = (NatsConnection) standardConnection(options);
            StreamConfiguration sc = StreamConfiguration.builder()
                .name(stream)
                .storageType(StorageType.File) // file since we are killing the server and bringing it back up.
                .subjects(subject).build();
            nc.jetStreamManagement().addStream(sc);

            jsPublish(nc, subject, 10000);

            ConsumeOptions consumeOptions = ConsumeOptions.builder()
                .batchSize(100) // small batch size means more round trips
                .expiresIn(1500) // idle heartbeat is half of this, alarm time is 3 * ihb
                .build();

            OrderedConsumerConfiguration ocConfig = new OrderedConsumerConfiguration().filterSubjects(subject);
            streamContext = nc.getStreamContext(stream);
            orderedConsumerContext = streamContext.createOrderedConsumer(ocConfig);
            assertNull(orderedConsumerContext.getConsumerName());
            mcon = orderedConsumerContext.consume(consumeOptions, handler);
            firstConsumerName = validateConsumerNameForOrdered(orderedConsumerContext, mcon, null);

            sleep(500); // time enough to get some messages
        }

        assertTrue(allInOrder.get());
        int count1 = atomicCount.get();
        assertTrue(count1 > 0);
        assertEquals(count1, nextExpectedSequence.get());

        // reconnect and get some more messages
        try (NatsTestServer ignored = new NatsTestServer(port, false, true)) {
            standardConnectionWait(nc);
            sleep(6000); // long enough to get messages and for the hb alarm to have tripped
        }
        validateConsumerNameForOrdered(orderedConsumerContext, mcon, null);
        assertNotEquals(firstConsumerName, orderedConsumerContext.getConsumerName());

        assertTrue(allInOrder.get());
        int count2 = atomicCount.get();
        assertTrue(count2 > count1);
        assertEquals(count2, nextExpectedSequence.get());

        sleep(6000); // enough delay before reconnect to trip hb alarm again
        try (NatsTestServer ignored = new NatsTestServer(port, false, true)) {
            standardConnectionWait(nc);
            sleep(6000); // long enough to get messages and for the hb alarm to have tripped

            try {
                nc.jetStreamManagement().deleteStream(stream); // it was a file stream clean it up
            }
            catch (JetStreamApiException ignore) {
                // in GH actions this fails sometimes
            }
        }

        assertTrue(allInOrder.get());
        int count3 = atomicCount.get();
        assertTrue(count3 > count2);
        assertEquals(count3, nextExpectedSequence.get());
    }
}
