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

import org.junit.jupiter.api.Test;

import java.time.Duration;
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

    @SuppressWarnings("SameParameterValue")
    private static void pushTestMessages(WriterMessageQueue q, int count) {
        for (int i = 0; i < count; i++) {
            q.push(getTestMessage());
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static void pushTestMessages(ConsumerMessageQueue q, int count) {
        for (int i = 0; i < count; i++) {
            q.push(getTestMessage());
        }
    }

    private static WriterMessageQueue getWriterMessageQueue() {
        return getWriterMessageQueue(-1);
    }

    private static WriterMessageQueue getWriterMessageQueue(int maxMessagesInOutgoingQueue) {
        return new WriterMessageQueue(maxMessagesInOutgoingQueue, false, Duration.ofMillis(500));
    }

    @Test
    public void testEmptyPopConsumer() throws InterruptedException {
        ConsumerMessageQueue q = new ConsumerMessageQueue();
        NatsMessage msg = q.pop(null);
        assertNull(msg);
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
    public void testTimeoutZeroConsumer() throws InterruptedException {
        ConsumerMessageQueue q = new ConsumerMessageQueue();
        NatsMessage expected = getTestMessage();
        q.push(expected);
        NatsMessage actual = q.pop(Duration.ZERO);
        assertNotNull(actual);
        assertEquals(expected, actual);
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
        WriterMessageQueue q = getWriterMessageQueue();
        Thread[] threads = new Thread[10];
        for (int i = 0; i < 10; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < 10; j++) {
                    q.push(getTestMessage());
                    sleep(10);
                }
            });
        }
        for (int i = 0; i < 10; i++) {
            threads[i].start();
        }
        for (int i = 0; i < 10; i++) {
            threads[i].join();
        }

        validateAccumulate(100, q.accumulate(-1, 101, Duration.ofMillis(500)));
    }

    @Test
    public void testMultipleReaders() throws InterruptedException {
        ConsumerMessageQueue q = new ConsumerMessageQueue();
        AtomicInteger allCount = new AtomicInteger(0);
        AtomicInteger[] counts = new AtomicInteger[10];
        Thread[] threads = new Thread[10];

        pushTestMessages(q, 1000);

        for (int i = 0; i < 10; i++) {
            counts[i] = new AtomicInteger(0);
            final int ii = i;
            threads[i] = new Thread(() -> {
                try {
                    do {
                        if (q.pop(Duration.ofMillis(500)) == null) {
                            return;
                        }
                        allCount.incrementAndGet();
                        counts[ii].incrementAndGet();
                        sleep(10);
                    } while (allCount.get() < 100);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });
        }
        for (int i = 0; i < 10; i++) {
            threads[i].start();
        }
        for (int i = 0; i < 10; i++) {
            threads[i].join();
        }

        for (int i = 0; i < 10; i++) {
            assertTrue(counts[i].get() > 1);
        }
    }

    private void validateAccumulate(int expected, NatsMessage head) {
        while (expected > 0) {
            assertNotNull(head);
            head = head.next;
            expected--;
        }
        assertNull(head);
    }

    private void validateAccumulate(NatsMessage head, NatsMessage... expecteds) {
        NatsMessage msg = head;
        for (NatsMessage expected : expecteds) {
            assertNotNull(msg);
            assertEquals(expected, msg);
            msg = msg.next;
        }
        assertNull(msg);
    }

    @Test
    public void testAccumulate() throws InterruptedException {
        WriterMessageQueue q = getWriterMessageQueue();
        validateAccumulate(0, q.accumulate(-1, 10, null));

        NatsMessage expected1 = getTestMessage();
        NatsMessage expected2 = getTestMessage();

        q.push(expected1);
        validateAccumulate(q.accumulate(-1, 2, null), expected1);

        q.push(expected1);
        q.push(expected2);
        validateAccumulate(q.accumulate(-1, 2, null), expected1, expected2);
    }

    @Test
    public void testAccumulateLimitCount() throws InterruptedException {
        WriterMessageQueue q = getWriterMessageQueue();
        pushTestMessages(q, 7);
        long maxBytesToAccumulate = TEST_MESSAGE_BYTES * 10;
        validateAccumulate(3, q.accumulate(maxBytesToAccumulate, 3, null));
        validateAccumulate(3, q.accumulate(maxBytesToAccumulate, 3, null));
        validateAccumulate(1, q.accumulate(maxBytesToAccumulate, 3, null));
    }

    @Test
    public void testAccumulateLimitBytes() throws InterruptedException {
        WriterMessageQueue q = getWriterMessageQueue();
        pushTestMessages(q, 7);
        long maxBytesToAccumulate = TEST_MESSAGE_BYTES * 4 - 1;
        validateAccumulate(3, q.accumulate(maxBytesToAccumulate, 100, null));
        validateAccumulate(3, q.accumulate(maxBytesToAccumulate, 100, null));
        validateAccumulate(1, q.accumulate(maxBytesToAccumulate, 100, null));
    }

    @Test
    public void testLength() throws InterruptedException {
        WriterMessageQueue q = getWriterMessageQueue();
        NatsMessage msg1 = getTestMessage();
        NatsMessage msg2 = getTestMessage();
        NatsMessage msg3 = getTestMessage();

        q.push(msg1);
        assertEquals(1, q.length());
        q.push(msg2);
        assertEquals(2, q.length());
        q.push(msg3);
        assertEquals(3, q.length());
        q.accumulate(-1, 1, Duration.ofMillis(500));
        assertEquals(2, q.length());
        q.accumulate(-1, 100, null);
        assertEquals(0, q.length());
    }

    @Test
    public void testSizeInBytes() throws InterruptedException {
        WriterMessageQueue q = getWriterMessageQueue();
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

        validateAccumulate(q.accumulate(TEST_MESSAGE_BYTES + 1, 100, null), msg1);
        assertEquals(TEST_MESSAGE_BYTES * 2, q.sizeInBytes());

        validateAccumulate(q.accumulate(TEST_MESSAGE_BYTES * 3, 100, null), msg2, msg3, notCounted);
        assertEquals(0, q.sizeInBytes());
    }

    @Test
    public void testFilteringAndCounting() {
        NatsMessage test = getTestMessage();
        ProtocolMessage proto = new ProtocolMessage("proto".getBytes());
        MarkerMessage marker = new MarkerMessage("marker");

        long sizeM = test.getSizeInBytes();
        long sizeP = proto.getSizeInBytes();

        WriterMessageQueue q = getWriterMessageQueue();
        q.push(test);
        q.push(proto);
        q.queueMarkerMessage(marker);

        assertEquals(3, q.queueSize()); // test, proto, marker
        assertEquals(2, q.length());    // test, proto
        assertEquals(sizeM + sizeP, q.sizeInBytes());

        q.pause(); // this poisons the queue, adding another Marker Message
        q.filter();
        q.resume();

        assertEquals(3, q.queueSize()); // test, marker, poison
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

    private static NatsMessage getTestFilteredMessage(String id, final boolean filterOnStop) {
        if (filterOnStop) {
            return new ProtocolMessage(("customFilter" + id).getBytes());
        }
        return new NatsMessage("customFilter" + id, null, null);
    }

    private static void _testFiltered(int filtered) throws InterruptedException {
        NatsMessage msg1 = getTestFilteredMessage("1", filtered == 1);
        NatsMessage msg2 = getTestFilteredMessage("2", filtered == 2);
        NatsMessage msg3 = getTestFilteredMessage("3", filtered == 3);

        long size1 = msg1.getSizeInBytes();
        long size2 = msg2.getSizeInBytes();
        long size3 = msg3.getSizeInBytes();
        long sizeAll = size1 + size2 + size3;
        long sizeAfter = filtered == 1 ? size2 + size3 : size1 * 2;

        WriterMessageQueue q = getWriterMessageQueue();
        q.push(msg1);
        q.push(msg2);
        q.push(msg3);

        assertEquals(3, q.length());
        assertEquals(sizeAll, q.sizeInBytes());

        q.pause();
        q.filter();
        q.resume();

        assertEquals(2, q.length());
        assertEquals(sizeAfter, q.sizeInBytes());

        NatsMessage head = q.accumulate(-1, 3, Duration.ofMillis(500));
        if (filtered != 1) {
            assertEquals(msg1, head);
            head = head.next;
        }
        if (filtered != 2) {
            assertEquals(msg2, head);
            head = head.next;
        }
        if (filtered != 3) {
            assertEquals(msg3, head);
        }
    }

    @Test
    public void testPausedAccumulate() throws InterruptedException {
        WriterMessageQueue q = getWriterMessageQueue();
        q.pause();
        NatsMessage msg = q.accumulate(1, 1, null);
        assertNull(msg);
    }

    @Test
    public void testExceptionWhenQueueIsFull() {
        WriterMessageQueue q = getWriterMessageQueue(2);
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
        WriterMessageQueue q = new WriterMessageQueue(2, true, Duration.ofMillis(500));
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

        validateAccumulate(q.accumulate(-1, 10, null), msg1, msg2);
        assertEquals(0, q.length());
        assertEquals(0, q.sizeInBytes());

        validateAccumulate(0, q.accumulate(-1, 10, null));
    }

    @Test
    public void testClear() throws InterruptedException {
        WriterMessageQueue q = getWriterMessageQueue();
        NatsMessage msg = getTestMessage();

        assertTrue(q.push(msg));
        assertEquals(1, q.length());
        assertEquals(TEST_MESSAGE_BYTES, q.sizeInBytes());

        q.clear();
        assertEquals(0, q.length());
        assertEquals(0, q.sizeInBytes());

        validateAccumulate(0, q.accumulate(-1, 10, null));
    }

    @Test
    public void testStateWriter() throws InterruptedException {
        WriterMessageQueue q = getWriterMessageQueue();
        q.push(getTestMessage());
        assertTrue(q.isRunning());
        assertFalse(q.isPaused());
        assertFalse(q.isDraining());
        assertFalse(q.isDrained());

        q.pause(); // this poisons the queue, adding another Marker Message
        assertFalse(q.isRunning());
        assertTrue(q.isPaused());
        assertFalse(q.isDraining());
        assertFalse(q.isDrained());

        q.resume();
        assertTrue(q.isRunning());
        assertFalse(q.isPaused());
        assertFalse(q.isDraining());
        assertFalse(q.isDrained());

        q.drain();
        assertTrue(q.isRunning()); // still running while draining
        assertFalse(q.isPaused());
        assertTrue(q.isDraining());
        assertFalse(q.isDrained());

        q.accumulate(-1, 1, Duration.ofSeconds(1));
        assertTrue(q.isRunning()); // still running while draining
        assertFalse(q.isPaused());
        assertTrue(q.isDraining());
        assertTrue(q.isDrained());
    }

    @Test
    public void testStateConsumer() throws InterruptedException {
        ConsumerMessageQueue q = new ConsumerMessageQueue();
        q.push(getTestMessage());
        assertTrue(q.isRunning());
        assertFalse(q.isPaused());
        assertFalse(q.isDraining());
        assertFalse(q.isDrained());

        q.pause(); // this poisons the queue, adding another Marker Message
        assertFalse(q.isRunning());
        assertTrue(q.isPaused());
        assertFalse(q.isDraining());
        assertFalse(q.isDrained());

        q.resume();
        assertTrue(q.isRunning());
        assertFalse(q.isPaused());
        assertFalse(q.isDraining());
        assertFalse(q.isDrained());

        q.drain();
        assertTrue(q.isRunning()); // still running while draining
        assertFalse(q.isPaused());
        assertTrue(q.isDraining());
        assertFalse(q.isDrained());

        q.pop(null);
        assertTrue(q.isRunning()); // still running while draining
        assertFalse(q.isPaused());
        assertTrue(q.isDraining());
        assertTrue(q.isDrained());
    }
}