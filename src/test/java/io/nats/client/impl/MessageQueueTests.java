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
    static final Duration REQUEST_CLEANUP_INTERVAL = Duration.ofMillis(100);

    private static NatsMessage getTestMessage() {
        return new NatsMessage(variant(), null, null);
    }

    private static final long TEST_MESSAGE_BYTES = getTestMessage().getSizeInBytes();

    private static void pushTestMessages(MessageQueue q, int count) {
        for (int i = 0; i < count; i++) {
            q.push(getTestMessage());
        }
    }

    @Test
    public void testEmptyPop() throws InterruptedException {
        MessageQueue q = new MessageQueue(false, REQUEST_CLEANUP_INTERVAL);
        NatsMessage msg = q.popNow();
        assertNull(msg);
        assertFalse(q.isSingleReaderMode());
    }

    @Test
    public void testAccumulateThrowsOnNonSingleReader() {
        assertThrows(IllegalStateException.class, () -> {
            MessageQueue q = new MessageQueue(false, REQUEST_CLEANUP_INTERVAL);
            q.push(getTestMessage());
            q.accumulate(100, 1, null);
        });
    }

    @Test
    public void testPushPop() throws InterruptedException {
        MessageQueue q = new MessageQueue(false, REQUEST_CLEANUP_INTERVAL);
        NatsMessage expected = getTestMessage();
        q.push(expected);
        NatsMessage actual = q.popNow();
        assertEquals(expected, actual);
    }

    @Test
    public void testTimeout() throws InterruptedException {
        long waitTime = 500;
        MessageQueue q = new MessageQueue(false, REQUEST_CLEANUP_INTERVAL);
        long start = System.nanoTime();
        NatsMessage msg = q.pop(Duration.ofMillis(waitTime));
        long end = System.nanoTime();
        long actual = (end - start) / 1_000_000L;

        // Time out should occur within 50% of the expected
        // This could be a flaky test, how can we fix it?
        // Using wide boundary to try to help.
        assertTrue(actual > (waitTime * 0.5) && actual < (waitTime * 1.5));
        assertNull(msg);
    }

    @Test
    public void testTimeoutZero() throws InterruptedException {
        MessageQueue q = new MessageQueue(false, REQUEST_CLEANUP_INTERVAL);
        NatsMessage expected = getTestMessage();
        q.push(expected);
        NatsMessage msg = q.pop(Duration.ZERO);
        assertNotNull(msg);
    }

    @Test
    public void testInterupt() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = new MessageQueue(false, REQUEST_CLEANUP_INTERVAL);
        Thread t = new Thread(() -> {try {Thread.sleep(100);}catch(Exception e){/**/} q.pause();});
        t.start();
        NatsMessage msg = q.pop(Duration.ZERO);
        assertNull(msg);
    }

    @Test
    public void testReset() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = new MessageQueue(false, REQUEST_CLEANUP_INTERVAL);
        Thread t = new Thread(() -> {try {Thread.sleep(100);}catch(Exception e){/**/} q.pause();});
        t.start();
        NatsMessage msg = q.pop(Duration.ZERO);
        assertNull(msg);

        NatsMessage expected = getTestMessage();
        q.push(expected);

        msg = q.pop(Duration.ZERO);
        assertNull(msg); // Haven't reset yet

        q.resume();
        msg = q.popNow();
        assertEquals(expected, msg);
    }

    @Test
    public void testPopBeforeTimeout() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = new MessageQueue(false, REQUEST_CLEANUP_INTERVAL);

        Thread t = new Thread(() -> {
            try {
                Thread.sleep(500);
                q.push(getTestMessage());
            } catch (Exception exp) {
                // eat the exception, test will fail
            }
        });
        t.start();

        // Thread timing, so could be flaky
        NatsMessage msg = q.pop(Duration.ofMillis(5000));
        assertNotNull(msg);
    }

    @Test
    public void testMultipleWriters() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = new MessageQueue(false, REQUEST_CLEANUP_INTERVAL);
        int threads = 10;

        for (int i=0;i<threads;i++) {
            Thread t = new Thread(() -> q.push(getTestMessage()));
            t.start();
        }

        for (int i=0;i<threads;i++) {
            NatsMessage msg = q.pop(Duration.ofMillis(500));
            assertNotNull(msg);
        }

        NatsMessage msg = q.popNow();
        assertNull(msg);
    }

    @Test
    public void testMultipleReaders() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = new MessageQueue(false, REQUEST_CLEANUP_INTERVAL);
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

        NatsMessage msg = q.popNow();
        assertNull(msg);
    }

    @Test
    public void testMultipleReadersAndWriters() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = new MessageQueue(false, REQUEST_CLEANUP_INTERVAL);
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

        NatsMessage msg = q.popNow();
        assertNull(msg);
    }

    @Test
    public void testMultipleReaderWriters() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = new MessageQueue(false, REQUEST_CLEANUP_INTERVAL);
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

        NatsMessage msg = q.popNow();
        assertNull(msg);
    }

    @Test
    public void testEmptyAccumulate() throws InterruptedException {
        MessageQueue q = new MessageQueue(true, REQUEST_CLEANUP_INTERVAL);
        NatsMessage msg = q.accumulate(1,1,null);
        assertNull(msg);
        assertTrue(q.isSingleReaderMode());
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
    public void testAccumulateOnCount() throws InterruptedException {
        MessageQueue q = new MessageQueue(true, REQUEST_CLEANUP_INTERVAL);
        pushTestMessages(q, 7);
        long maxBytesToAccumulate = TEST_MESSAGE_BYTES * 10;
        validateAccumulate(3, q.accumulate(maxBytesToAccumulate, 3, null));
        validateAccumulate(3, q.accumulate(maxBytesToAccumulate, 3, null));
        validateAccumulate(1, q.accumulate(maxBytesToAccumulate, 3, null));
    }

    @Test
    public void testAccumulateOnSize() throws InterruptedException {
        MessageQueue q = new MessageQueue(true, REQUEST_CLEANUP_INTERVAL);
        pushTestMessages(q, 7);
        long maxBytesToAccumulate = TEST_MESSAGE_BYTES * 4 - 1;
        validateAccumulate(3, q.accumulate(maxBytesToAccumulate, 100, null));
        validateAccumulate(3, q.accumulate(maxBytesToAccumulate, 100, null));
        validateAccumulate(1, q.accumulate(maxBytesToAccumulate, 100, null));
    }

    @Test
    public void testAccumulateAndPop() throws InterruptedException {
        MessageQueue q = new MessageQueue(true, REQUEST_CLEANUP_INTERVAL);
        pushTestMessages(q, 4);
        long maxBytesToAccumulate = TEST_MESSAGE_BYTES * 10;
        validateAccumulate(3, q.accumulate(maxBytesToAccumulate, 3, null));
        assertNotNull(q.popNow());
        validateAccumulate(0, q.accumulate(maxBytesToAccumulate, 3, null));
    }

    @Test
    public void testMultipleWritersOneAccumulator() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = new MessageQueue(true, REQUEST_CLEANUP_INTERVAL);
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

        NatsMessage msg = q.popNow();
        assertNull(msg);
    }

    @Test
    public void testInterruptAccumulate() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = new MessageQueue(true, REQUEST_CLEANUP_INTERVAL);
        Thread t = new Thread(() -> {try {Thread.sleep(100);}catch(Exception ignored){} q.pause();});
        t.start();
        NatsMessage msg = q.accumulate(100,100, Duration.ZERO);
        assertNull(msg);
    }

    @Test
    public void testLength() throws InterruptedException {
        MessageQueue q = new MessageQueue(true, REQUEST_CLEANUP_INTERVAL);
        NatsMessage msg1 = getTestMessage();
        NatsMessage msg2 = getTestMessage();
        NatsMessage msg3 = getTestMessage();

        q.push(msg1);
        assertEquals(1, q.length());
        q.push(msg2);
        assertEquals(2, q.length());
        q.push(msg3);
        assertEquals(3, q.length());
        q.popNow();
        assertEquals(2, q.length());
        q.accumulate(100, 100, null);
        assertEquals(0, q.length());
    }
    
    @Test
    public void testSizeInBytes() throws InterruptedException {
        MessageQueue q = new MessageQueue(true, REQUEST_CLEANUP_INTERVAL);
        NatsMessage msg1 = getTestMessage();
        NatsMessage msg2 = getTestMessage();
        NatsMessage msg3 = getTestMessage();
        MessageQueue.TerminalMessage notCounted = new MessageQueue.TerminalMessage("not-counted");

        q.push(msg1);
        assertEquals(TEST_MESSAGE_BYTES, q.sizeInBytes());

        q.push(msg2);
        assertEquals(TEST_MESSAGE_BYTES * 2, q.sizeInBytes());

        q.push(msg3);
        assertEquals(TEST_MESSAGE_BYTES * 3, q.sizeInBytes());

        q.queueTerminalMessage(notCounted);
        assertEquals(TEST_MESSAGE_BYTES * 3, q.sizeInBytes());

        q.popNow();
        assertEquals(TEST_MESSAGE_BYTES * 2, q.sizeInBytes());

        validateAccumulate(3, q.accumulate(100, 100, null));
        assertEquals(0, q.sizeInBytes());
    }

    @Test
    public void testSizeInBytesWithData() throws InterruptedException {
        MessageQueue q = new MessageQueue(true, REQUEST_CLEANUP_INTERVAL);

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
        q.popNow();
        expected -= msg1.getSizeInBytes();
        assertEquals(expected, q.sizeInBytes());
        q.accumulate(1000,100, null); expected = 0;
        assertEquals(expected, q.sizeInBytes());
    }

    @Test
    public void testDrainTo() {
        MessageQueue q1 = new MessageQueue(true, REQUEST_CLEANUP_INTERVAL);

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

        MessageQueue q2 = new MessageQueue(true, REQUEST_CLEANUP_INTERVAL);
        q1.drainTo(q2);
        assertEquals(3, q2.length());
        assertEquals(expected, q2.sizeInBytes());
        assertEquals(0, q1.length());
        assertEquals(0, q1.sizeInBytes());
    }

    @Test
    public void testTerminal() throws InterruptedException {
        NatsMessage msg = getTestMessage();
        MessageQueue.TerminalMessage term = new MessageQueue.TerminalMessage("term");

        long size = msg.getSizeInBytes();

        MessageQueue q = new MessageQueue(true, REQUEST_CLEANUP_INTERVAL);
        q.push(msg);
        q.queueTerminalMessage(term);

        assertEquals(2, q.queue.size());
        assertEquals(1, q.length());
        assertEquals(size, q.sizeInBytes());

        q.pause();
        q.filterOnStop();
        q.resume();

        assertEquals(1, q.queue.size());
        assertEquals(1, q.length());
        assertEquals(size, q.sizeInBytes());
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

    static NatsMessage getCustomFilter(String id, final boolean filterOnStop) {
        return new NatsMessage("customFilter" + id, null, null) {
            @Override
            boolean isFilterOnStop() {
                return filterOnStop;
            }
        };
    }

    private static void _testFiltered(int filtered) throws InterruptedException {
        NatsMessage msg1 = getCustomFilter("1", filtered == 1);
        NatsMessage msg2 = getCustomFilter("2", filtered == 2);
        NatsMessage msg3 = getCustomFilter("3", filtered == 3);

        long size = msg1.getSizeInBytes(); // will all be the same size

        MessageQueue q = new MessageQueue(true, REQUEST_CLEANUP_INTERVAL);
        q.push(msg1);
        q.push(msg2);
        q.push(msg3);

        assertEquals(3, q.length());
        assertEquals(size * 3, q.sizeInBytes());

        q.pause();
        q.filterOnStop();
        q.resume();

        assertEquals(2, q.length());
        assertEquals(size * 2, q.sizeInBytes());

        q.pause();
        q.filterOnStop();
        q.resume();

        assertEquals(2, q.length());
        assertEquals(size * 2, q.sizeInBytes());

        if (filtered != 1) {
            assertEquals(msg1, q.popNow());
        }
        if (filtered != 2) {
            assertEquals(msg2, q.popNow());
        }
        if (filtered != 3) {
            assertEquals(msg3, q.popNow());
        }
    }

    @Test
    public void testPausedAccumulate() throws InterruptedException {
        MessageQueue q = new MessageQueue(true, REQUEST_CLEANUP_INTERVAL);
        q.pause();
        NatsMessage msg = q.accumulate(1, 1, null);
        assertNull(msg);
    }

    @Test
    public void testThrowOnFilterIfRunning() {
        MessageQueue q = new MessageQueue(true, REQUEST_CLEANUP_INTERVAL);
        assertThrows(IllegalStateException.class, q::filterOnStop);
    }

    @Test
    public void testExceptionWhenQueueIsFull() {
        MessageQueue q  = new MessageQueue(true, 2, false, REQUEST_CLEANUP_INTERVAL);
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
        MessageQueue q  = new MessageQueue(true, 2, true, REQUEST_CLEANUP_INTERVAL);
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

        Message m = q.popNow();
        assertEquals(msg1, m);
        assertEquals(1, q.length());
        assertEquals(TEST_MESSAGE_BYTES, q.sizeInBytes());

        m = q.popNow();
        assertEquals(msg2, m);
        assertEquals(0, q.length());
        assertEquals(0, q.sizeInBytes());

        assertNull(q.popNow());
    }
}
