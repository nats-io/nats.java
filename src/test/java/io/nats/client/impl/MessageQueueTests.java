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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

public class MessageQueueTests {
    @Test
    public void testLinkedList() throws InterruptedException {
        MessageQueue q = new MessageQueue();
        NatsMessage msg1 = new NatsMessage("one");
        NatsMessage msg2 = new NatsMessage("two");
        NatsMessage msg3 = new NatsMessage("three");

        q.push(msg1);
        q.push(msg2);
        q.push(msg3);

        assertTrue(msg1.prev == msg2); assertTrue(msg1.next == null);
        assertTrue(msg2.prev == msg3); assertTrue(msg2.next == msg1);
        assertTrue(msg3.prev == null); assertTrue(msg3.next == msg2);

        NatsMessage msg = q.popNow();
        assertTrue(msg == msg1);
        assertTrue(msg1.prev == null); assertTrue(msg1.next == null);
        assertTrue(msg2.prev == msg3); assertTrue(msg2.next == null);
        assertTrue(msg3.prev == null); assertTrue(msg3.next == msg2);

        msg = q.popNow();
        assertTrue(msg == msg2);
        assertTrue(msg2.prev == null); assertTrue(msg2.next == null);
        assertTrue(msg3.prev == null); assertTrue(msg3.next == null);
        
        msg = q.popNow();
        assertTrue(msg == msg3);
        assertTrue(msg3.prev == null); assertTrue(msg3.next == null);

        msg = q.popNow();
        assertNull(msg);
    }

    @Test
    public void testLinkedListUnderAccumulate() throws InterruptedException {
        MessageQueue q = new MessageQueue();
        NatsMessage msg1 = new NatsMessage("one");
        NatsMessage msg2 = new NatsMessage("two");
        NatsMessage msg3 = new NatsMessage("three");

        q.push(msg1);
        q.push(msg2);
        q.push(msg3);

        assertTrue(msg1.prev == msg2); assertTrue(msg1.next == null);
        assertTrue(msg2.prev == msg3); assertTrue(msg2.next == msg1);
        assertTrue(msg3.prev == null); assertTrue(msg3.next == msg2);
        assertTrue(q.length() == 3);

        NatsMessage msg = q.accumulate(1000, 2, null, null);
        assertTrue(msg == msg1);
        assertTrue(msg1.prev == msg2); assertTrue(msg1.next == null);
        assertTrue(msg2.prev == null); assertTrue(msg2.next == msg1);
        assertTrue(msg3.prev == null); assertTrue(msg3.next == null);
        assertTrue(q.length() == 1);

        msg = q.accumulate(1000, 2, null, null);
        assertTrue(msg == msg3);
        assertTrue(msg3.prev == null); assertTrue(msg3.next == null);
        assertTrue(q.length() == 0);

        msg = q.popNow();
        assertNull(msg);
    }

    @Test
    public void testEmptyPop() throws InterruptedException {
        MessageQueue q = new MessageQueue();
        NatsMessage msg = q.popNow();
        assertNull(msg);
    }

    @Test
    public void testPushPop() throws InterruptedException {
        MessageQueue q = new MessageQueue();
        NatsMessage expected = new NatsMessage("test");
        q.push(expected);
        NatsMessage actual = q.popNow();
        assertEquals(expected, actual);
    }

    @Test
    public void testTimeout() throws InterruptedException {
        long waitTime = 500;
        MessageQueue q = new MessageQueue();
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
    public void testInterupt() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = new MessageQueue();
        Thread t = new Thread(() -> {try {Thread.sleep(100);}catch(Exception e){} q.interrupt();});
        t.start();
        NatsMessage msg = q.pop(Duration.ZERO);
        assertNull(msg);
    }

    @Test
    public void testReset() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = new MessageQueue();
        Thread t = new Thread(() -> {try {Thread.sleep(100);}catch(Exception e){} q.interrupt();});
        t.start();
        NatsMessage msg = q.pop(Duration.ZERO);
        assertNull(msg);

        NatsMessage expected = new NatsMessage("test");
        q.push(expected);

        msg = q.pop(Duration.ZERO);
        assertNull(msg); // Haven't reset yet

        q.reset();
        msg = q.popNow();
        assertEquals(expected, msg);
    }

    @Test
    public void testPopBeforeTimeout() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = new MessageQueue();

        Thread t = new Thread(() -> {
            try {
                Thread.sleep(100);
                q.push(new NatsMessage("test"));
            } catch (Exception exp) {
                // eat the exception, test will fail
            }
        });
        t.start();

        // Thread timing, so could be flaky
        NatsMessage msg = q.pop(Duration.ofMillis(200));
        assertNotNull(msg);
    }

    @Test
    public void testMultipleWriters() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = new MessageQueue();
        int threads = 10;

        for (int i=0;i<threads;i++) {
            Thread t = new Thread(() -> {q.push(new NatsMessage("test"));});
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
        MessageQueue q = new MessageQueue();
        int threads = 10;
        AtomicInteger count = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(threads);

        for (int i=0;i<threads;i++) {
            q.push(new NatsMessage("test"));
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
        MessageQueue q = new MessageQueue();
        int threads = 10;
        int msgPerThread = 10;
        AtomicInteger count = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(threads * msgPerThread);

        for (int i=0;i<threads;i++) {
            Thread t = new Thread(() -> {
                                for (int j=0;j<msgPerThread;j++) {
                                    q.push(new NatsMessage("test"));
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
        MessageQueue q = new MessageQueue();
        int threads = 10;
        int msgPerThread = 1_000;
        AtomicInteger count = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(threads * msgPerThread);

        // Each thread writes 1 and reads one, could be a different one
        for (int i=0;i<threads;i++) {
            Thread t = new Thread(() -> {
                                for (int j=0;j<msgPerThread;j++) {
                                    q.push(new NatsMessage("test"));
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
        MessageQueue q = new MessageQueue();
        NatsMessage msg = q.accumulate(1,1,null,null);
        assertNull(msg);
    }

    @Test
    public void testSingleAccumulate() throws InterruptedException {
        MessageQueue q = new MessageQueue();
        q.push(new NatsMessage("PING"));
        NatsMessage msg = q.accumulate(100,1,null,null);
        assertNotNull(msg);
    }

    @Test
    public void testMultiAccumulate() throws InterruptedException {
        MessageQueue q = new MessageQueue();
        q.push(new NatsMessage("PING"));
        q.push(new NatsMessage("PING"));
        q.push(new NatsMessage("PING"));
        NatsMessage msg = q.accumulate(100,3,null,null);
        assertNotNull(msg);
    }

    private void checkCount(NatsMessage first, int expected) {
        while (expected > 0) {
            assertNotNull(first);
            first = first.prev;
            expected--;
        }

        assertNull(first);
    }

    @Test
    public void testPartialAccumulateOnCount() throws InterruptedException {
        MessageQueue q = new MessageQueue();
        q.push(new NatsMessage("PING"));
        q.push(new NatsMessage("PING"));
        q.push(new NatsMessage("PING"));
        q.push(new NatsMessage("PING"));
        NatsMessage msg = q.accumulate(100,3,null,null);
        checkCount(msg, 3);

        msg = q.accumulate(100, 3, null, null); // should only get the last one
        checkCount(msg, 1);
    }

    @Test
    public void testMultipleAccumulateOnCount() throws InterruptedException {
        MessageQueue q = new MessageQueue();
        q.push(new NatsMessage("PING"));
        q.push(new NatsMessage("PING"));
        q.push(new NatsMessage("PING"));
        q.push(new NatsMessage("PING"));
        q.push(new NatsMessage("PING"));
        q.push(new NatsMessage("PING"));
        NatsMessage msg = q.accumulate(100,2,null,null);
        checkCount(msg, 2);

        msg = q.accumulate(100, 2, null, null);
        checkCount(msg, 2);

        msg = q.accumulate(100, 2, null, null);
        checkCount(msg, 2);
    }
    

    @Test
    public void testPartialAccumulateOnSize() throws InterruptedException {
        MessageQueue q = new MessageQueue();
        q.push(new NatsMessage("PING"));
        q.push(new NatsMessage("PING"));
        q.push(new NatsMessage("PING"));
        q.push(new NatsMessage("PING"));
        NatsMessage msg = q.accumulate(20,100,null,null); // each one is 6 so 20 should be 3 messages
        checkCount(msg, 3);

        msg = q.accumulate(20,100, null, null); // should only get the last one
        checkCount(msg, 1);
    }

    @Test
    public void testMultipleAccumulateOnSize() throws InterruptedException {
        MessageQueue q = new MessageQueue();
        q.push(new NatsMessage("PING"));
        q.push(new NatsMessage("PING"));
        q.push(new NatsMessage("PING"));
        q.push(new NatsMessage("PING"));
        q.push(new NatsMessage("PING"));
        q.push(new NatsMessage("PING"));
        NatsMessage msg = q.accumulate(14,100,null,null); // each one is 6 so 14 should be 2 messages
        checkCount(msg, 2);

        msg = q.accumulate(14,100, null, null);
        checkCount(msg, 2);

        msg = q.accumulate(14,100, null, null);
        checkCount(msg, 2);
    }
    
    @Test
    public void testAccumulateAndPop() throws InterruptedException {
        MessageQueue q = new MessageQueue();
        q.push(new NatsMessage("PING"));
        q.push(new NatsMessage("PING"));
        q.push(new NatsMessage("PING"));
        q.push(new NatsMessage("PING"));
        NatsMessage msg = q.accumulate(100,3,null,null);
        checkCount(msg, 3);

        msg = q.popNow();
        checkCount(msg, 1);

        msg = q.accumulate(100, 3, null, null); // should be empty
        checkCount(msg, 0);
    }

    @Test
    public void testMultipleWritersOneAccumulator() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = new MessageQueue();
        int threads = 7;

        for (int i=0;i<threads;i++) {
            Thread t = new Thread(() -> {
                q.push(new NatsMessage("test"));
                q.push(new NatsMessage("test"));
                q.push(new NatsMessage("test"));
                q.push(new NatsMessage("test"));
                q.push(new NatsMessage("test"));
            });
            t.start();
        }

        for (int i=0;i<threads;i++) {
            NatsMessage msg = q.accumulate(100, 5, Duration.ofMillis(5000), Duration.ofMillis(5000));
            checkCount(msg, 5);
        }
        
        NatsMessage msg = q.popNow();
        assertNull(msg);
    }

    @Test
    public void testMultipleAccumulatorsAndWriters() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = new MessageQueue();
        int threads = 5;
        int msgPerThread = 30;
        AtomicInteger count = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(threads * 2);

        for (int i=0;i<threads;i++) {
            Thread t = new Thread(() -> {
                                for (int j=0;j<msgPerThread;j++) {
                                    q.push(new NatsMessage("test"));
                                }});
            t.start();
        }

        for (int i=0;i<threads * 2;i++) {
            Thread t = new Thread(() -> {
                                    try{
                                        NatsMessage msg = q.accumulate(100 * msgPerThread, msgPerThread/2, Duration.ofMillis(5000), Duration.ofMillis(5000));
                                        checkCount(msg, msgPerThread/2);
                                        count.getAndAdd(msgPerThread/2);
                                        latch.countDown();
                                    }catch(Exception e){
                                    }
                                });
            t.start();
        }

        latch.await(30, TimeUnit.SECONDS);
        assertEquals(threads * msgPerThread, count.get());
        
        NatsMessage msg = q.popNow();
        assertNull(msg);
    }

    @Test
    public void testAccumulateTimeouts() throws InterruptedException {
        long waitTime = 500;
        MessageQueue q = new MessageQueue();
        long start = System.nanoTime();
        NatsMessage msg = q.accumulate(100, 100, Duration.ofMillis(waitTime), Duration.ofMillis(waitTime));
        long end = System.nanoTime();
        long actual = (end - start) / 1_000_000L;

        // Time out should occur within 50% of the expected
        // This could be a flaky test, how can we fix it?
        // Using wide boundary to try to help.
        assertTrue(actual > (waitTime * 0.5) && actual < (waitTime * 1.5));
        assertNull(msg);
    }

    @Test
    public void testAccumulateTimeoutShorterThanTimeout() throws InterruptedException {
        long waitTime = 500;
        MessageQueue q = new MessageQueue();
        long start = System.nanoTime();
        NatsMessage msg = q.accumulate(100, 100, Duration.ofMillis(waitTime / 10), Duration.ofMillis(waitTime));
        long end = System.nanoTime();
        long actual = (end - start) / 1_000_000L;

        // Time out should occur within 50% of the expected
        // This could be a flaky test, how can we fix it?
        // Using wide boundary to try to help.
        assertTrue(actual > (waitTime * 0.5) && actual < (waitTime * 1.5));
        assertNull(msg);
    }

    @Test
    public void testInteruptAccumulate() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = new MessageQueue();
        Thread t = new Thread(() -> {try {Thread.sleep(100);}catch(Exception e){} q.interrupt();});
        t.start();
        NatsMessage msg = q.accumulate(100,100, Duration.ZERO, Duration.ZERO);
        assertNull(msg);
    }

    @Test
    public void testAccumulateOnTimeout() throws InterruptedException {
        // Possible flaky test, since we can't be sure of thread timing
        MessageQueue q = new MessageQueue();

        Thread t = new Thread(() -> {
            try {
                Thread.sleep(200);
                q.push(new NatsMessage("test"));
                Thread.sleep(200);
                q.push(new NatsMessage("test"));
                Thread.sleep(200);
                q.push(new NatsMessage("test"));
            } catch (Exception exp) {
                // eat the exception, test will fail
                exp.printStackTrace();
            }
        });
        t.start();

        // Thread timing, so could be flaky
        NatsMessage msg = q.accumulate(100, 10, Duration.ofMillis(20), Duration.ofMillis(30));
        assertNull(msg);

        // Only wait long enough for 1 to accumulate
        msg = q.accumulate(100, 10, Duration.ofMillis(20), Duration.ofMillis(200));
        assertNotNull(msg);
        checkCount(msg, 1);

        t.join();
    }
}