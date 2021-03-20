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

import io.nats.client.impl.NatsMessage.ProtocolMessage;
import org.junit.jupiter.api.Test;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class MessageQueueTests {
    byte[] PING = "PING".getBytes();
    byte[] ONE = "one".getBytes();
    byte[] TWO = "two".getBytes();
    byte[] THREE = "three".getBytes();

    @Test
    public void testEmptyPop() throws InterruptedException {
        MessageQueue q = new MessageQueue(false);
        NatsMessage msg = q.popNow();
        assertNull(msg);
        assertFalse(q.isSingleReaderMode());
    }

    @Test
    public void testAccumulateThrowsOnNonSingleReader() {
        assertThrows(IllegalStateException.class, () -> {
            WriteMessageQueue q = new WriteMessageQueue(false);
            q.push(new ProtocolMessage(PING));
            q.accumulate(100,1,null);
        });
    }

    @Test
    public void testPushPop() throws InterruptedException {
        MessageQueue q = new MessageQueue(false);
        NatsMessage expected = new ProtocolMessage(PING);
        q.push(expected);
        NatsMessage actual = q.popNow();
        assertEquals(expected, actual);
    }

    @Test
    public void testTimeout() throws InterruptedException {
        long waitTime = 500;
        MessageQueue q = new MessageQueue(false);
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
        MessageQueue q = new MessageQueue(false);
        NatsMessage expected = new ProtocolMessage(PING);
        q.push(expected);
        NatsMessage msg = q.pop(Duration.ZERO);
        assertNotNull(msg);
    }

    @Test
    public void testInterupt() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = new MessageQueue(false);
        Thread t = new Thread(() -> {try {Thread.sleep(100);}catch(Exception e){/**/} q.pause();});
        t.start();
        NatsMessage msg = q.pop(Duration.ZERO);
        assertNull(msg);
    }

    @Test
    public void testReset() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = new MessageQueue(false);
        Thread t = new Thread(() -> {try {Thread.sleep(100);}catch(Exception e){/**/} q.pause();});
        t.start();
        NatsMessage msg = q.pop(Duration.ZERO);
        assertNull(msg);

        NatsMessage expected = new ProtocolMessage(PING);
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
        MessageQueue q = new MessageQueue(false);

        Thread t = new Thread(() -> {
            try {
                Thread.sleep(500);
                q.push(new ProtocolMessage(PING));
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
        MessageQueue q = new MessageQueue(false);
        int threads = 10;

        for (int i=0;i<threads;i++) {
            Thread t = new Thread(() -> {q.push(new ProtocolMessage(PING));});
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
        MessageQueue q = new MessageQueue(false);
        int threads = 10;
        AtomicInteger count = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(threads);

        for (int i=0;i<threads;i++) {
            q.push(new ProtocolMessage(PING));
        }

        for (int i=0;i<threads;i++) {
            Thread t = new Thread(() -> {
                                try{NatsMessage msg = q.pop(Duration.ofMillis(500)); 
                                if(msg!=null){count.incrementAndGet();}
                                latch.countDown();}catch(Exception e){}});
            t.start();
        }

        latch.await(500, TimeUnit.MILLISECONDS);

        assertEquals(threads, count.get());
        
        NatsMessage msg = q.popNow();
        assertNull(msg);
    }

    @Test
    public void testMultipleReadersAndWriters() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = new MessageQueue(false);
        int threads = 10;
        int msgPerThread = 10;
        AtomicInteger count = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(threads * msgPerThread);

        for (int i=0;i<threads;i++) {
            Thread t = new Thread(() -> {
                                for (int j=0;j<msgPerThread;j++) {
                                    q.push(new ProtocolMessage(PING));
                                }});
            t.start();
        }

        for (int i=0;i<threads;i++) {
            Thread t = new Thread(() -> {
                                for (int j=0;j<msgPerThread;j++) {
                                    try{NatsMessage msg = q.pop(Duration.ofMillis(300)); 
                                    if(msg!=null){count.incrementAndGet();}
                                    latch.countDown();}catch(Exception e){}
                                }});
            t.start();
        }

        latch.await(5, TimeUnit.SECONDS);

        assertEquals(threads * msgPerThread, count.get());
        
        NatsMessage msg = q.popNow();
        assertNull(msg);
    }

    @Test
    public void testMultipleReaderWriters() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = new MessageQueue(false);
        int threads = 10;
        int msgPerThread = 1_000;
        AtomicInteger count = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(threads * msgPerThread);

        // Each thread writes 1 and reads one, could be a different one
        for (int i=0;i<threads;i++) {
            Thread t = new Thread(() -> {
                                for (int j=0;j<msgPerThread;j++) {
                                    q.push(new ProtocolMessage(PING));
                                    try{NatsMessage msg = q.pop(Duration.ofMillis(300)); 
                                        if(msg!=null){count.incrementAndGet();}
                                        latch.countDown();}catch(Exception e){}
                                }});
            t.start();
        }

        latch.await(5, TimeUnit.SECONDS);

        assertEquals(threads * msgPerThread, count.get());
        
        NatsMessage msg = q.popNow();
        assertNull(msg);
    }

    @Test
    public void testEmptyAccumulate() throws InterruptedException {
        WriteMessageQueue q = new WriteMessageQueue(true);
        MessageQueue.AccumulateResult result = q.accumulate(1,1,null);
        assertNull(result);
        assertTrue(q.isSingleReaderMode());
    }

    @Test
    public void testSingleAccumulate() throws InterruptedException {
        WriteMessageQueue q = new WriteMessageQueue(true);
        q.push(new ProtocolMessage(PING));
        MessageQueue.AccumulateResult result = q.accumulate(100,1,null);
        assertNotNull(result);
    }

    @Test
    public void testMultiAccumulate() throws InterruptedException {
        WriteMessageQueue q = new WriteMessageQueue(true);
        q.push(new ProtocolMessage(PING));
        q.push(new ProtocolMessage(PING));
        q.push(new ProtocolMessage(PING));
        MessageQueue.AccumulateResult result = q.accumulate(100,3,null);
        assertNotNull(result);
    }

    private void checkCount(NatsMessage first, int expected) {
        while (expected > 0) {
            assertNotNull(first);
            first = first.next;
            expected--;
        }

        assertNull(first);
    }

    @Test
    public void testPartialAccumulateOnCount() throws InterruptedException {
        WriteMessageQueue q = new WriteMessageQueue(true);
        q.push(new ProtocolMessage(PING));
        q.push(new ProtocolMessage(PING));
        q.push(new ProtocolMessage(PING));
        q.push(new ProtocolMessage(PING));
        MessageQueue.AccumulateResult result = q.accumulate(100,3,null);
        checkCount(result.head, 3);

        result = q.accumulate(100, 3, null); // should only get the last one
        checkCount(result.head, 1);
    }

    @Test
    public void testMultipleAccumulateOnCount() throws InterruptedException {
        WriteMessageQueue q = new WriteMessageQueue(true);
        q.push(new ProtocolMessage(PING));
        q.push(new ProtocolMessage(PING));
        q.push(new ProtocolMessage(PING));
        q.push(new ProtocolMessage(PING));
        q.push(new ProtocolMessage(PING));
        q.push(new ProtocolMessage(PING));
        MessageQueue.AccumulateResult result = q.accumulate(100,2,null);
        checkCount(result.head, 2);

        result = q.accumulate(100, 2, null);
        checkCount(result.head, 2);

        result = q.accumulate(100, 2, null);
        checkCount(result.head, 2);
    }
    

    @Test
    public void testPartialAccumulateOnSize() throws InterruptedException {
        WriteMessageQueue q = new WriteMessageQueue(true);
        q.push(new ProtocolMessage(PING));
        q.push(new ProtocolMessage(PING));
        q.push(new ProtocolMessage(PING));
        q.push(new ProtocolMessage(PING));
        MessageQueue.AccumulateResult result = q.accumulate(20,100,null); // each one is 6 so 20 should be 3 messages
        checkCount(result.head, 3);

        result = q.accumulate(20,100, null); // should only get the last one
        checkCount(result.head, 1);
    }

    @Test
    public void testMultipleAccumulateOnSize() throws InterruptedException {
        WriteMessageQueue q = new WriteMessageQueue(true);
        q.push(new ProtocolMessage(PING));
        q.push(new ProtocolMessage(PING));
        q.push(new ProtocolMessage(PING));
        q.push(new ProtocolMessage(PING));
        q.push(new ProtocolMessage(PING));
        q.push(new ProtocolMessage(PING));
        MessageQueue.AccumulateResult result = q.accumulate(14,100,null); // each one is 6 so 14 should be 2 messages
        checkCount(result.head, 2);

        result = q.accumulate(14,100, null);
        checkCount(result.head, 2);

        result = q.accumulate(14,100, null);
        checkCount(result.head, 2);
    }
    
    @Test
    public void testAccumulateAndPop() throws InterruptedException {
        WriteMessageQueue q = new WriteMessageQueue(true);
        q.push(new ProtocolMessage(PING));
        q.push(new ProtocolMessage(PING));
        q.push(new ProtocolMessage(PING));
        q.push(new ProtocolMessage(PING));
        MessageQueue.AccumulateResult result = q.accumulate(100,3,null);
        checkCount(result.head, 3);

        NatsMessage msg = q.popNow();
        checkCount(msg, 1);

        result = q.accumulate(100, 3, null); // should be empty
        assertNull(result);
    }

    @Test
    public void testMultipleWritersOneAccumulator() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        WriteMessageQueue q = new WriteMessageQueue(true);
        int threads = 4;
        int msgPerThread = 77;
        int msgCount = threads * msgPerThread;
        AtomicInteger sent = new AtomicInteger(0);
        AtomicInteger count = new AtomicInteger(0);
        int tries = msgCount;

        for (int i=0;i<threads;i++) {
            Thread t = new Thread(() -> {
                for (int j=0;j<msgPerThread;j++) {
                    q.push(new ProtocolMessage(PING));
                    sent.incrementAndGet();
                };
            });
            t.start();
        }


        while (count.get() < msgCount && (tries > 0 || sent.get() < msgCount)) {
            MessageQueue.AccumulateResult result = q.accumulate(5000, 10, Duration.ofMillis(5000));
            NatsMessage msg = result.head;
            while (msg != null) {
                count.incrementAndGet();
                msg = msg.next;
            }
            tries--;
            Thread.sleep(1);
        }

        assertEquals(msgCount, sent.get());
        assertEquals(msgCount, count.get());

        NatsMessage msg = q.popNow();
        assertNull(msg);
    }

    @Test
    public void testInteruptAccumulate() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        WriteMessageQueue q = new WriteMessageQueue(true);
        Thread t = new Thread(() -> {try {Thread.sleep(100);}catch(Exception e){} q.pause();});
        t.start();
        MessageQueue.AccumulateResult result = q.accumulate(100,100, Duration.ZERO);
        assertNull(result);
    }
    
    @Test
    public void testLength() throws InterruptedException {
        WriteMessageQueue q = new WriteMessageQueue(true);
        NatsMessage msg1 = new ProtocolMessage(PING);
        NatsMessage msg2 = new ProtocolMessage(PING);
        NatsMessage msg3 = new ProtocolMessage(PING);

        q.push(msg1);
        assertEquals(1, q.length());
        q.push(msg2);
        assertEquals(2, q.length());
        q.push(msg3);
        assertEquals(3, q.length());
        q.popNow();
        assertEquals(2, q.length());
        q.accumulate(100,100, null);
        assertEquals(0, q.length());
    }
    
    @Test
    public void testSizeInBytes() throws InterruptedException {
        WriteMessageQueue q = new WriteMessageQueue(true);
        NatsMessage msg1 = new ProtocolMessage(ONE);
        NatsMessage msg2 = new ProtocolMessage(TWO);
        NatsMessage msg3 = new ProtocolMessage(THREE);
        long expected = 0;

        q.push(msg1);    expected += msg1.getSizeInBytes();
        assertEquals(expected, q.sizeInBytes());
        q.push(msg2);    expected += msg2.getSizeInBytes();
        assertEquals(expected, q.sizeInBytes());
        q.push(msg3);    expected += msg3.getSizeInBytes();
        assertEquals(expected, q.sizeInBytes());
        q.popNow();      expected -= msg1.getSizeInBytes();
        assertEquals(expected, q.sizeInBytes());
        q.accumulate(100,100, null); expected = 0;
        assertEquals(expected, q.sizeInBytes());
    }

    @Test
    public void testFilterTail() throws InterruptedException, UnsupportedEncodingException {
        WriteMessageQueue q = new WriteMessageQueue(true);
        NatsMessage msg1 = new ProtocolMessage(ONE);
        NatsMessage msg2 = new ProtocolMessage(TWO);
        NatsMessage msg3 = new ProtocolMessage(THREE);
        byte[] expected = "one".getBytes(StandardCharsets.UTF_8);

        q.push(msg1);
        q.push(msg2);
        q.push(msg3);

        long before = q.sizeInBytes();
        q.pause();
        q.filter(msg -> msg.getProtocolBytes().equals(expected));
        q.resume();
        long after = q.sizeInBytes();

        assertEquals(2,q.length());
        assertEquals(before, after + expected.length + 2);
        assertEquals(q.popNow(), msg2);
        assertEquals(q.popNow(), msg3);
    }

    @Test
    public void testFilterHead() throws InterruptedException, UnsupportedEncodingException {
        WriteMessageQueue q = new WriteMessageQueue(true);
        NatsMessage msg1 = new ProtocolMessage(ONE);
        NatsMessage msg2 = new ProtocolMessage(TWO);
        NatsMessage msg3 = new ProtocolMessage(THREE);
        byte[] expected = "three".getBytes(StandardCharsets.UTF_8);

        q.push(msg1);
        q.push(msg2);
        q.push(msg3);

        long before = q.sizeInBytes();
        q.pause();
        q.filter(msg -> msg.getProtocolBytes().equals(expected));
        q.resume();
        long after = q.sizeInBytes();

        assertEquals(2,q.length());
        assertEquals(before, after + expected.length + 2);
        assertEquals(q.popNow(), msg1);
        assertEquals(q.popNow(), msg2);
    }

    @Test
    public void testFilterMiddle() throws InterruptedException, UnsupportedEncodingException {
        WriteMessageQueue q = new WriteMessageQueue(true);
        NatsMessage msg1 = new ProtocolMessage(ONE);
        NatsMessage msg2 = new ProtocolMessage(TWO);
        NatsMessage msg3 = new ProtocolMessage(THREE);
        byte[] expected = "two".getBytes(StandardCharsets.UTF_8);

        q.push(msg1);
        q.push(msg2);
        q.push(msg3);

        long before = q.sizeInBytes();
        q.pause();
        q.filter(msg -> msg.getProtocolBytes().equals(expected));
        q.resume();
        long after = q.sizeInBytes();

        assertEquals(2,q.length());
        assertEquals(before, after + expected.length + 2);
        assertEquals(q.popNow(), msg1);
        assertEquals(q.popNow(), msg3);
    }

    @Test
    public void testPausedAccumulate() throws InterruptedException {
        WriteMessageQueue q = new WriteMessageQueue(true);
        q.pause();
        MessageQueue.AccumulateResult result = q.accumulate(1,1,null);
        assertNull(result);
    }

    @Test
    public void testThrowOnFilterIfRunning() {
        assertThrows(IllegalStateException.class, () -> {
            WriteMessageQueue q = new WriteMessageQueue(true);
            q.filter(msg -> true);
        });
    }

    @Test
    public void testExceptionWhenQueueIsFull() {
        MessageQueue q  = new MessageQueue(true, 2);
        NatsMessage msg1 = new ProtocolMessage(ONE);
        NatsMessage msg2 = new ProtocolMessage(TWO);
        NatsMessage msg3 = new ProtocolMessage(THREE);

        assertTrue(q.push(msg1));
        assertTrue(q.push(msg2));
        try {
            q.push(msg3);
            fail("Expected " + IllegalStateException.class.getSimpleName());
        } catch (IllegalStateException e) {
            assertEquals("Output queue is full 2", e.getMessage());
        }
    }

    @Test
    public void testDiscardMessageWhenQueueFull() {
        MessageQueue q  = new MessageQueue(true, 2, true);
        NatsMessage msg1 = new ProtocolMessage(ONE);
        NatsMessage msg2 = new ProtocolMessage(TWO);
        NatsMessage msg3 = new ProtocolMessage(THREE);

        assertTrue(q.push(msg1));
        assertTrue(q.push(msg2));
        assertFalse(q.push(msg3));
    }
}