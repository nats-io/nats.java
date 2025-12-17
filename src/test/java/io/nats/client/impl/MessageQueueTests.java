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

import io.nats.client.Message;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.nats.client.support.NatsConstants.OUTPUT_QUEUE_IS_FULL;
import static io.nats.client.utils.TestBase.sleep;
import static io.nats.client.utils.TestBase.variant;
import static org.junit.jupiter.api.Assertions.*;

public class MessageQueueTests {
    private static NatsMessage getTestMessage() {
        return new NatsMessage(variant(), null, null);
    }

    private static final long TEST_MESSAGE_BYTES = getTestMessage().getSizeInBytes();

    private static void pushTestMessages(MessageQueue q, int count) {
        for (int i = 0; i < count; i++) {
            q.push(getTestMessage());
        }
    }

    private static MessageQueue getWriterMessageQueue() {
        return getWriterMessageQueue(-1);
    }

    private static MessageQueue getWriterMessageQueue(int maxMessagesInOutgoingQueue) {
        return new MessageQueue(maxMessagesInOutgoingQueue, false, Duration.ofMillis(100), null);
    }

    @Test
    public void testEmptyPopWriter() throws InterruptedException {
        MessageQueue q = getWriterMessageQueue();
        NatsMessage msg = q.pop(null);
        assertNull(msg);
    }

    @Test
    public void testEmptyPopConsumer() throws InterruptedException {
        ConsumerMessageQueue q = new ConsumerMessageQueue();
        NatsMessage msg = q.pop(null);
        assertNull(msg);
    }

    @Test
    public void testPushPopWriter() throws InterruptedException {
        MessageQueue q = getWriterMessageQueue();
        NatsMessage expected = getTestMessage();
        q.push(expected);
        NatsMessage actual = q.pop(null);
        assertEquals(expected, actual);
    }

    @Test
    public void testPushPopConsumer() throws InterruptedException {
        ConsumerMessageQueue q = new ConsumerMessageQueue();
        NatsMessage expected = getTestMessage();
        q.push(expected);
        NatsMessage actual = q.pop(null);
        assertEquals(expected, actual);
    }

    @Test
    public void testTimeoutWriter() throws InterruptedException {
        long waitTimeNanos = 100 * 1_000_000L;
        MessageQueue q = getWriterMessageQueue();
        long start = System.nanoTime();
        NatsMessage msg = q.pop(Duration.ofNanos(waitTimeNanos));
        long end = System.nanoTime();
        long elapsed = end - start;
        assertNull(msg);
        assertTrue(elapsed > waitTimeNanos);
    }

    @Test
    public void testTimeoutConsumer() throws InterruptedException {
        long waitTimeNanos = 100 * 1_000_000L;
        ConsumerMessageQueue q = new ConsumerMessageQueue();
        long start = System.nanoTime();
        NatsMessage msg = q.pop(Duration.ofNanos(waitTimeNanos));
        long end = System.nanoTime();
        long elapsed = end - start;
        assertNull(msg);
        assertTrue(elapsed > waitTimeNanos);
    }

    @Test
    public void testTimeoutZeroWriter() throws InterruptedException {
        MessageQueue q = getWriterMessageQueue();
        NatsMessage expected = getTestMessage();
        q.push(expected);
        NatsMessage actual = q.pop(Duration.ZERO);
        assertNotNull(actual);
        assertEquals(expected, actual);
    }

    @Test
    public void testTimeoutZeroConsumer() throws InterruptedException {
        ConsumerMessageQueue q = new ConsumerMessageQueue();
        NatsMessage expected = getTestMessage();
        q.push(expected);
        NatsMessage actual = q.pop(Duration.ZERO);
        assertNotNull(actual);
        assertEquals(expected, actual);
    }

    @Test
    public void testInterruptWriter() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = getWriterMessageQueue();
        Thread t = new Thread(() -> {
            try { Thread.sleep(100); } catch (Exception ignore) {}
            q.pause();
        });
        t.start();
        NatsMessage msg = q.pop(Duration.ZERO); // zero is wait forever
        assertNull(msg);
    }

    @Test
    public void testResetWriter() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = getWriterMessageQueue();
        Thread t = new Thread(() -> {
            try { Thread.sleep(100); } catch (Exception ignore) {}
            q.pause();
        });
        t.start();
        NatsMessage msg = q.pop(Duration.ZERO);
        assertNull(msg);

        NatsMessage expected = getTestMessage();
        q.push(expected);

        msg = q.pop(Duration.ZERO);
        assertNull(msg); // Haven't reset yet

        q.resume();
        msg = q.pop(null);
        assertEquals(expected, msg);
    }

    @Test
    public void testPauseResumeConsumer() throws InterruptedException {
        ConsumerMessageQueue q = new ConsumerMessageQueue();
        Thread t = new Thread(() -> {
            try { Thread.sleep(100); } catch (Exception ignore) {}
            q.pause();
        });
        t.start();
        NatsMessage msg = q.pop(Duration.ZERO);
        assertNull(msg);

        NatsMessage expected = getTestMessage();
        q.push(expected);

        msg = q.pop(Duration.ZERO);
        assertNull(msg); // Haven't reset yet

        q.resume();
        msg = q.pop(null);
        assertEquals(expected, msg);
    }

    @Test
    public void testDrainConsumer() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        ConsumerMessageQueue q = new ConsumerMessageQueue();
        Thread t = new Thread(() -> {
            try { Thread.sleep(100); } catch (Exception ignore) {}
            q.drain();
        });
        t.start();
        NatsMessage msg = q.pop(Duration.ZERO);
        assertNull(msg);

        NatsMessage expected = getTestMessage();
        q.push(expected);

        msg = q.pop(Duration.ZERO);
        assertEquals(expected, msg);
    }

    @Test
    public void testMultipleWriters() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = getWriterMessageQueue();
        int threads = 10;

        for (int i=0;i<threads;i++) {
            Thread t = new Thread(() -> q.push(getTestMessage()));
            t.start();
        }

        for (int i=0;i<threads;i++) {
            NatsMessage msg = q.pop(Duration.ofMillis(500));
            assertNotNull(msg);
        }

        NatsMessage msg = q.pop(null);
        assertNull(msg);
    }

    @Test
    public void testMultipleReaders() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = getWriterMessageQueue();
        int threads = 10;
        AtomicInteger count = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(threads);

        pushTestMessages(q, threads);

        for (int i=0;i<threads;i++) {
            Thread t = new Thread(() -> {
                try {
                    NatsMessage msg = q.pop(Duration.ofMillis(500));
                    if (msg !=null ) {
                        count.incrementAndGet();
                    }
                    latch.countDown();
                }
                catch (Exception ignored){}
            });
            t.start();
        }

        assertTrue(latch.await(500, TimeUnit.MILLISECONDS));

        assertEquals(threads, count.get());

        NatsMessage msg = q.pop(null);
        assertNull(msg);
    }

    @Test
    public void testMultipleReadersAndWriters() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = getWriterMessageQueue();
        int threads = 10;
        int msgPerThread = 10;
        AtomicInteger count = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(threads * msgPerThread);

        for (int i=0;i<threads;i++) {
            Thread t = new Thread(() -> {
                pushTestMessages(q, msgPerThread);
            });
            t.start();
        }

        for (int i=0;i<threads;i++) {
            Thread t = new Thread(() -> {
                for (int j=0;j<msgPerThread;j++) {
                    try{
                        NatsMessage msg = q.pop(Duration.ofMillis(300));
                        if( msg != null) {
                            count.incrementAndGet();
                        }
                        latch.countDown();
                    }
                    catch(Exception ignored){}
                }
            });
            t.start();
        }

        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertEquals(threads * msgPerThread, count.get());

        NatsMessage msg = q.pop(null);
        assertNull(msg);
    }

    @Test
    public void testMultipleReaderWriters() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = getWriterMessageQueue();
        int threads = 10;
        int msgPerThread = 1_000;
        AtomicInteger count = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(threads * msgPerThread);

        // Each thread writes 1 and reads one, could be a different one
        for (int i=0;i<threads;i++) {
            Thread t = new Thread(() -> {
                for (int j = 0; j < msgPerThread; j++) {
                    q.push(getTestMessage());
                    try{
                        NatsMessage msg = q.pop(Duration.ofMillis(300));
                        if ( msg != null) {
                            count.incrementAndGet();
                        }
                        latch.countDown();
                    }
                    catch(Exception ignored){}
                }
            });
            t.start();
        }

        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertEquals(threads * msgPerThread, count.get());

        NatsMessage msg = q.pop(null);
        assertNull(msg);
    }

    @Test
    public void testEmptyAccumulate() throws InterruptedException {
        MessageQueue q = getWriterMessageQueue();
        NatsMessage msg = q.accumulate(1,1,null);
        assertNull(msg);
    }

    private void validateAccumulate(int expected, NatsMessage msg) {
        while (expected > 0) {
            assertNotNull(msg);
            msg = msg.next;
            expected--;
        }
        assertNull(msg);
    }

    @Test
    public void testAccumulateLimitCount() throws InterruptedException {
        MessageQueue q = getWriterMessageQueue();
        pushTestMessages(q, 7);
        long maxBytesToAccumulate = TEST_MESSAGE_BYTES * 10;
        validateAccumulate(3, q.accumulate(maxBytesToAccumulate, 3, null));
        validateAccumulate(3, q.accumulate(maxBytesToAccumulate, 3, null));
        validateAccumulate(1, q.accumulate(maxBytesToAccumulate, 3, null));
    }

    @Test
    public void testAccumulateLimitBytes() throws InterruptedException {
        MessageQueue q = getWriterMessageQueue();
        pushTestMessages(q, 7);
        long maxBytesToAccumulate = TEST_MESSAGE_BYTES * 4 - 1;
        validateAccumulate(3, q.accumulate(maxBytesToAccumulate, 100, null));
        validateAccumulate(3, q.accumulate(maxBytesToAccumulate, 100, null));
        validateAccumulate(1, q.accumulate(maxBytesToAccumulate, 100, null));
    }

    @Test
    public void testAccumulateAndPop() throws InterruptedException {
        MessageQueue q = getWriterMessageQueue();
        pushTestMessages(q, 4);
        long maxBytesToAccumulate = TEST_MESSAGE_BYTES * 10;
        validateAccumulate(3, q.accumulate(maxBytesToAccumulate, 3, null));
        assertNotNull(q.pop(null));
        validateAccumulate(0, q.accumulate(maxBytesToAccumulate, 3, null));
    }

    @Test
    public void testMultipleWritersOneAccumulator() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = getWriterMessageQueue();
        int threads = 4;
        int msgPerThread = 77;
        int msgCount = threads * msgPerThread;
        AtomicInteger sent = new AtomicInteger(0);
        AtomicInteger count = new AtomicInteger(0);
        int tries = msgCount;

        for (int i=0;i<threads;i++) {
            Thread t = new Thread(() -> {
                for (int j=0;j<msgPerThread;j++) {
                    q.push(getTestMessage());
                    sent.incrementAndGet();
                }
            });
            t.start();
        }

        while (count.get() < msgCount && (tries > 0 || sent.get() < msgCount)) {
            NatsMessage msg = q.accumulate(5000, 10, Duration.ofMillis(5000));

            while (msg != null) {
                count.incrementAndGet();
                msg = msg.next;
            }
            tries--;
            sleep(1);
        }

        assertEquals(msgCount, sent.get());
        assertEquals(msgCount, count.get());

        NatsMessage msg = q.pop(null);
        assertNull(msg);
    }

    @Test
    public void testInterruptAccumulate() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = getWriterMessageQueue();
        Thread t = new Thread(() -> {try {Thread.sleep(100);}catch(Exception ignored){} q.pause();});
        t.start();
        NatsMessage msg = q.accumulate(100,100, Duration.ZERO);
        assertNull(msg);
    }

    @Test
    public void testLength() throws InterruptedException {
        MessageQueue q = getWriterMessageQueue();
        NatsMessage msg1 = getTestMessage();
        NatsMessage msg2 = getTestMessage();
        NatsMessage msg3 = getTestMessage();

        q.push(msg1);
        assertEquals(1, q.length());
        q.push(msg2);
        assertEquals(2, q.length());
        q.push(msg3);
        assertEquals(3, q.length());
        q.pop(null);
        assertEquals(2, q.length());
        q.accumulate(100, 100, null);
        assertEquals(0, q.length());
    }

    @Test
    public void testSizeInBytes() throws InterruptedException {
        MessageQueue q = getWriterMessageQueue();
        NatsMessage msg1 = getTestMessage();
        NatsMessage msg2 = getTestMessage();
        NatsMessage msg3 = getTestMessage();
        MarkerMessage notCounted = new MarkerMessage("not-counted");

        q.push(msg1);
        assertEquals(TEST_MESSAGE_BYTES, q.sizeInBytes());

        q.push(msg2);
        assertEquals(TEST_MESSAGE_BYTES * 2, q.sizeInBytes());

        q.push(msg3);
        assertEquals(TEST_MESSAGE_BYTES * 3, q.sizeInBytes());

        q.queueMarkerMessage(notCounted);
        assertEquals(TEST_MESSAGE_BYTES * 3, q.sizeInBytes());

        q.pop(null);
        assertEquals(TEST_MESSAGE_BYTES * 2, q.sizeInBytes());

        validateAccumulate(3, q.accumulate(100, 100, null));
        assertEquals(0, q.sizeInBytes());
    }

    @Test
    public void testSizeInBytesWithData() throws InterruptedException {
        MessageQueue q = getWriterMessageQueue();

        String subject = "subj";
        String replyTo = "reply";
        Headers h = new Headers().add("Content-Type", "text/plain");
        NatsMessage msg1 = new NatsMessage(subject, null, h, new byte[8]);
        NatsMessage msg2 = new NatsMessage(subject, null, h, new byte[16]);
        NatsMessage msg3 = new NatsMessage(subject, replyTo, h, new byte[16]);
        long expected = 0;

        assertEquals(64, msg1.getSizeInBytes());
        assertEquals(72, msg2.getSizeInBytes());
        assertEquals(78, msg3.getSizeInBytes());

        q.push(msg1);
        expected += msg1.getSizeInBytes();
        assertEquals(expected, q.sizeInBytes());
        q.push(msg2);
        expected += msg2.getSizeInBytes();
        assertEquals(expected, q.sizeInBytes());
        q.push(msg3);
        expected += msg3.getSizeInBytes();
        assertEquals(expected, q.sizeInBytes());
        q.pop(null);
        expected -= msg1.getSizeInBytes();
        assertEquals(expected, q.sizeInBytes());
        q.accumulate(1000,100, null); expected = 0;
        assertEquals(expected, q.sizeInBytes());
    }

    @Test
    public void testDrainTo() {
        MessageQueue q1 = getWriterMessageQueue();

        String subject = "subj";
        String replyTo = "reply";
        Headers h = new Headers().add("Content-Type", "text/plain");
        NatsMessage msg1 = new NatsMessage(subject, null, h, new byte[8]);
        NatsMessage msg2 = new NatsMessage(subject, null, h, new byte[16]);
        NatsMessage msg3 = new NatsMessage(subject, replyTo, h, new byte[16]);
        long expected = 0;

        q1.push(msg1);
        expected += msg1.getSizeInBytes();
        assertEquals(1, q1.length());
        assertEquals(expected, q1.sizeInBytes());
        q1.push(msg2);
        expected += msg2.getSizeInBytes();
        assertEquals(2, q1.length());
        assertEquals(expected, q1.sizeInBytes());
        q1.push(msg3);
        expected += msg3.getSizeInBytes();
        assertEquals(3, q1.length());
        assertEquals(expected, q1.sizeInBytes());

        MessageQueue q2 = getWriterMessageQueue();
        q1.drainTo(q2);
        assertEquals(3, q2.length());
        assertEquals(expected, q2.sizeInBytes());
        assertEquals(0, q1.length());
        assertEquals(0, q1.sizeInBytes());
    }

    @Test
    public void testFilteringAndCounting() {
        NatsMessage test = getTestMessage();
        ProtocolMessage proto = new ProtocolMessage("proto".getBytes());
        MarkerMessage marker = new MarkerMessage("marker");

        long sizeM = test.getSizeInBytes();
        long sizeP = proto.getSizeInBytes();

        MessageQueue q = getWriterMessageQueue();
        q.push(test);
        q.push(proto);
        q.queueMarkerMessage(marker);

        assertEquals(3, q.size()); // test, proto, marker
        assertEquals(2, q.length());     // test, proto
        assertEquals(sizeM + sizeP, q.sizeInBytes());

        q.pause(); // this poisons the queue, adding another Marker Message
        assertEquals(4, q.size()); // test, proto, marker, poison
        assertEquals(2, q.length());     // test, proto
        assertEquals(sizeM + sizeP, q.sizeInBytes());

        q.filterOnStop(); // this ends up removing the proto message since it's filter on stop
        q.resume();

        assertEquals(3, q.size()); // test, marker, poison
        assertEquals(1, q.length());     // test
        assertEquals(sizeM, q.sizeInBytes());
    }

    @Test
    public void testFilterFirstIn() throws InterruptedException {
        _testFiltered(1);
    }

    @Test
    public void testFilterLastIn() throws InterruptedException {
        _testFiltered(3);
    }

    @Test
    public void testFilterMiddle() throws InterruptedException {
        _testFiltered(2);
    }

    static NatsMessage getMightBeFiltered(String id, final boolean filterOnStop) {
        if (filterOnStop) {
            return new ProtocolMessage("customFilter" + id);
        }
        return new NatsMessage("customFilter" + id, null, null);
    }

    private static void _testFiltered(int filtered) throws InterruptedException {
        NatsMessage msg1 = getMightBeFiltered("1", filtered == 1);
        NatsMessage msg2 = getMightBeFiltered("2", filtered == 2);
        NatsMessage msg3 = getMightBeFiltered("3", filtered == 3);

        long size1 = msg1.getSizeInBytes();
        long size2 = msg2.getSizeInBytes();
        long size3 = msg3.getSizeInBytes();
        long sizeAll = size1 + size2 + size3;
        long sizeAfter = filtered == 1 ? size2 + size3 : size1 * 2;

        MessageQueue q = getWriterMessageQueue();
        q.push(msg1);
        q.push(msg2);
        q.push(msg3);

        assertEquals(3, q.length());
        assertEquals(sizeAll, q.sizeInBytes());

        q.pause();
        q.filterOnStop();
        q.resume();

        assertEquals(2, q.length());
        assertEquals(sizeAfter, q.sizeInBytes());

        q.pause();
        q.filterOnStop();
        q.resume();

        assertEquals(2, q.length());
        assertEquals(sizeAfter, q.sizeInBytes());

        if (filtered != 1) {
            assertEquals(msg1, q.pop(null));
        }
        if (filtered != 2) {
            assertEquals(msg2, q.pop(null));
        }
        if (filtered != 3) {
            assertEquals(msg3, q.pop(null));
        }
    }

    @Test
    public void testPausedAccumulate() throws InterruptedException {
        MessageQueue q = getWriterMessageQueue();
        q.pause();
        NatsMessage msg = q.accumulate(1, 1, null);
        assertNull(msg);
    }

    @Test
    public void testThrowOnFilterIfRunning() {
        MessageQueue q = getWriterMessageQueue();
        assertThrows(IllegalStateException.class, q::filterOnStop);
    }

    @Test
    public void testExceptionWhenQueueIsFull() {
        MessageQueue q  = getWriterMessageQueue(2);
        NatsMessage msg1 = getTestMessage();
        NatsMessage msg2 = getTestMessage();
        NatsMessage msg3 = getTestMessage();

        assertTrue(q.push(msg1));
        assertEquals(1, q.length());
        assertEquals(TEST_MESSAGE_BYTES, q.sizeInBytes());

        assertTrue(q.push(msg2));
        assertEquals(2, q.length());
        assertEquals(TEST_MESSAGE_BYTES * 2, q.sizeInBytes());

        try {
            q.push(msg3);
            fail("Expected " + IllegalStateException.class.getSimpleName());
        } catch (IllegalStateException e) {
            assertEquals(OUTPUT_QUEUE_IS_FULL + "2", e.getMessage());
        }
    }

    @Test
    public void testDiscardMessageWhenQueueFull() throws InterruptedException {
        MessageQueue q  = new MessageQueue(2, true, Duration.ofMillis(100), null);
        NatsMessage msg1 = getTestMessage();
        NatsMessage msg2 = getTestMessage();
        NatsMessage msg3 = getTestMessage();

        assertTrue(q.push(msg1));
        assertEquals(1, q.length());
        assertEquals(TEST_MESSAGE_BYTES, q.sizeInBytes());

        assertTrue(q.push(msg2));
        assertEquals(2, q.length());
        assertEquals(TEST_MESSAGE_BYTES * 2, q.sizeInBytes());

        assertFalse(q.push(msg3));
        assertEquals(2, q.length());
        assertEquals(TEST_MESSAGE_BYTES * 2, q.sizeInBytes());

        Message m = q.pop(null);
        assertEquals(msg1, m);
        assertEquals(1, q.length());
        assertEquals(TEST_MESSAGE_BYTES, q.sizeInBytes());

        m = q.pop(null);
        assertEquals(msg2, m);
        assertEquals(0, q.length());
        assertEquals(0, q.sizeInBytes());

        assertNull(q.pop(null));
    }
}
