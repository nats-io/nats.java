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
        int msgPerThread = 100_000;
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
}