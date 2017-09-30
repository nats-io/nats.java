/*
 * The MIT License
 *
 * Copyright 2017 Apcera, Inc..
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package io.nats.client;

import io.nats.client.AsyncDeliveryQueue.MessageBatch;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class AsyncDeliveryQueueTest {

    private final AsyncDeliveryQueue q = new AsyncDeliveryQueue();
    static final AtomicLong IDS = new AtomicLong();
    static final AtomicLong MESSAGE_IDS = new AtomicLong();

    static final SubscriberKey keyA = new SubscriberKey("a");
    static final SubscriberKey keyB = new SubscriberKey("b");
    static final SubscriberKey keyC = new SubscriberKey("c");
    static final SubscriberKey keyD = new SubscriberKey("d");
    static final SubscriberKey keyE = new SubscriberKey("e");
    static final SubscriberKey keyF = new SubscriberKey("f");
    static final SubscriberKey keyG = new SubscriberKey("g");
    static final SubscriberKey[] keys = new SubscriberKey[]{keyA, keyB, keyC, keyD, keyE, keyF, keyG};

    @Test
    public void testSimpleReadWrite() throws Throwable {
        List<Message> batch = messageBatch(keyA, 20);
        for (Message m : batch) {
            q.add(1, m);
            q.add(2, m);
            q.add(3, m);
            q.add(4, m);
        }
        assertEquals(80, q.estimatedSize());
        assertTrue(q.contains(1));
        assertTrue(q.contains(2));
        assertTrue(q.contains(3));
        assertTrue(q.contains(4));
        q.remove(3);
        assertFalse(q.contains(3));
        List<Message> removed = q.removeAndCollect(4);
        assertEquals(batch, removed);

        assertEquals(40, q.estimatedSize());
        List<MessageBatch> batches = new ArrayList<>(2);
        q.get(batches);
        assertEquals(new HashSet<>(Arrays.asList(1L, 2L)), q.lockedKeys());
        assertEquals(2, batches.size());
        MessageBatch a = batches.get(0);
        MessageBatch b = batches.get(1);
        assertEquals(20, a.size());
        assertEquals(20, b.size());
        assertNotEquals(a.subscriberId(), b.subscriberId());
        assertTrue(a.subscriberId() == 1 || a.subscriberId() == 2);
        assertTrue(b.subscriberId() == 1 || b.subscriberId() == 2);
        a.close();
        b.close();
        Iterator<Message> ai = a.iterator();
        Iterator<Message> bi = b.iterator();
        for (int i = 0; i < 20; i++) {
            assertEquals(batch.get(i), ai.next());
            assertEquals(batch.get(i), bi.next());
        }
        batches.clear();
        q.get(batches, 10, TimeUnit.MILLISECONDS);
        assertTrue(batches.isEmpty());
        assertTrue(q.lockedKeys().isEmpty());
    }

    @Test
    public void testDeliveryOrder() throws InterruptedException {
        int threads = 16;
        ExecutorService threadPool = Executors.newFixedThreadPool(threads * 2);
        try {
            Phaser phaser = new Phaser(1);
            CountDownLatch submissionsDone = new CountDownLatch(1);
            int total = 0;
            List<Receiver> receivers = new LinkedList<>();
            List<Message> batchSet = new ArrayList<>();
            for (SubscriberKey key : keys) {
                List<Message> batch = messageBatch(key, 1567);
                batchSet.addAll(batch);
                total += batch.size();
            }
            threadPool.submit(new Deliverer(batchSet, 17, submissionsDone, phaser));
            MultiCountDownLatch receiversDone = new MultiCountDownLatch(total);
            for (int i = 0; i < threads; i++) {
                Receiver receiver = new Receiver(phaser, receiversDone);
                receivers.add(receiver);
                threadPool.submit(receiver);
            }
            // Release all the threads, so they are all launched at the same time
            phaser.arriveAndDeregister();
            // Wait for all the messages to be submitted to the queue
            submissionsDone.await(60, TimeUnit.SECONDS);
            // Wait until we've been notified that the received message count
            // matches the total number of messages
            receiversDone.await(60, TimeUnit.SECONDS);

            // Make sure the total received message count across all receivers
            // matches the total we expect
            int receivedTotal = 0;
            for (Receiver r : receivers) {
                receivedTotal += r.total();
            }
            // Ensure that each batch of messages only contained messages for one
            // subscriber, and that no messages arrived out of order
            for (Receiver r : receivers) {
                r.assertEachBatchIsForOneSubscriber();
                r.assertBatchesArrivedInOrder();
                r.shutdown = true;
            }

            // Now check total order - combine all the receivers maps of
            // batch to iteration into one, and do the same test
            Receiver r1 = new Receiver(receivers);
            r1.assertBatchesArrivedInOrder();

            // Check that the total really matches
            assertEquals(total, receivedTotal);
        } finally {
            threadPool.shutdownNow();
        }
    }

    long findQueueKeyFor(Message msg) {
        for (SubscriberKey k : keys) {
            if (k.name.equals(msg.getSubject())) {
                return k.id;
            }
        }
        throw new AssertionError("Unknown subject " + msg.getSubject());
    }

    class Deliverer implements Runnable {

        private final List<Message> toDeliver;
        private final int sleepEvery;
        private final CountDownLatch onDone;
        private final Phaser phaser;

        public Deliverer(List<Message> toDeliver, int sleepEvery, CountDownLatch onDone, Phaser phaser) {
            this.toDeliver = toDeliver;
            this.sleepEvery = sleepEvery;
            this.onDone = onDone;
            this.phaser = phaser;
        }

        @Override
        public void run() {
            // Wait for all the receivers to get to the party
            phaser.arriveAndAwaitAdvance();
            try {
                for (int i = 0; i < toDeliver.size(); i++) {
                    // Stagger batches with a sleep, so we don't blast them all into
                    // the queue and have them grabbed by one consumer
                    // All sleeps and batch size counts should be prime numbers to
                    // avoid cases where things look like they work if they are
                    // multiples of each other.
                    if (sleepEvery > 0 && (i % sleepEvery) == 0) {
                        try {
                            Thread.sleep(23);
                        } catch (InterruptedException ex) {
                            Logger.getLogger(AsyncDeliveryQueueTest.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                    Message msg = toDeliver.get(i);
                    long key = findQueueKeyFor(msg);
                    q.add(key, msg);
                    Thread.yield();
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                onDone.countDown();
            }
        }
    }

    // Provides a "timestamp" for each batch received, so we can test the
    // order later
    final AtomicInteger BATCHES = new AtomicInteger();

    class Receiver implements Runnable {

        final Map<Integer, List<MessageBatch>> batches = new HashMap<>();
        private final Phaser phaser;
        private final MultiCountDownLatch latch;
        volatile boolean shutdown;

        public Receiver(List<Receiver> others) {
            this(null, null);
            for (Receiver r : others) {
                batches.putAll(r.batches);
            }
        }

        public Receiver(Phaser phaser, MultiCountDownLatch latch) {
            this.phaser = phaser;
            this.latch = latch;
        }

        int total() {
            int result = 0;
            for (Map.Entry<Integer, List<MessageBatch>> e : batches.entrySet()) {
                for (MessageBatch mb : e.getValue()) {
                    result += mb.size();
                }
            }
            return result;
        }

        void assertBatchesArrivedInOrder() {
            Map<Long, List<Message>> msgs = new HashMap<>();
            List<Integer> iterations = new ArrayList<>(batches.keySet());
            Collections.sort(iterations);
            for (Integer key : iterations) {
                for (MessageBatch batch : batches.get(key)) {
                    List<Message> m = msgs.get(batch.subscriberId());
                    if (m == null) {
                        m = new ArrayList<>();
                        msgs.put(batch.subscriberId(), m);
                    }
                    for (Message msg : batch) {
                        m.add(msg);
                    }
                }
            }
            for (Map.Entry<Long, List<Message>> e : msgs.entrySet()) {
                assertListSorted("Out of order messages for " + e.getKey() + " in " + toWrappers(e.getValue()), e.getValue());
            }
        }

        void assertEachBatchIsForOneSubscriber() {
            for (Map.Entry<Integer, List<MessageBatch>> e : batches.entrySet()) {
                for (MessageBatch b : e.getValue()) {
                    Set<String> seenSubjects = new HashSet<>();
                    for (Message ms : b) {
                        seenSubjects.add(ms.getSubject());
                    }
                    assertTrue("Saw more than one subject: " + seenSubjects
                            + " in batch " + e.getKey() + ": " + b, seenSubjects.size() <= 1);
                }
            }
        }

        @Override
        public void run() {
            phaser.arriveAndAwaitAdvance();
            for (int ix = 0;; ix++) {
                List<MessageBatch> l = new ArrayList<>();
                try {
                    // Fetch from the queue
                    q.get(l);
                    int batch = BATCHES.getAndIncrement();
                    int total = 0;
                    for (MessageBatch b : l) {
                        total += b.size();
                        b.close();
                    }
                    if (total > 0) {
                        batches.put(batch, l);
                        latch.countDown(total);
                    }
                    if (ix % 37 == 0) {
                        Thread.sleep(200);
                    }
                    // Allow other receiver threads to get ahead
                    // May do nothing on some OS's
                    Thread.yield();
                } catch (Exception ex) {
                    if (shutdown) {
                        return;
                    }
                    Logger.getLogger(AsyncDeliveryQueueTest.class.getName()).log(Level.SEVERE, null, ex);
                    break;
                }
            }
        }

        public String toString() {
            // For verbose logging
            StringBuilder sb = new StringBuilder("RECEIVER\n");
            for (Map.Entry<Integer, List<MessageBatch>> e : batches.entrySet()) {
                sb.append("  Iter ").append(e.getKey()).append(" with ").append(e.getValue().size()).append(" batches\n");
                for (MessageBatch b : e.getValue()) {
                    for (Message msg : b) {
                        sb.append("    ").append(new ComparableMessageWrapper(msg).toString()).append("\n");
                    }
                }
            }
            return sb.toString();
        }
    }

    static List<Message> messageBatch(SubscriberKey key, int size) {
        List<Message> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            result.add(message(key, result));
        }
        return result;
    }

    static final Message message(SubscriberKey key, List<? super Message> addTo) {
        Message message = new Message(key.name, null, bytes());
        return message;
    }

    static List<ComparableMessageWrapper> toWrappers(List<? extends Message> msgs) {
        List<ComparableMessageWrapper> result = new ArrayList<>(msgs.size());
        for (Message msg : msgs) {
            result.add(new ComparableMessageWrapper(msg));
        }
        return result;
    }

    static void assertListSorted(String msg, List<Message> messages) {
        List<ComparableMessageWrapper> wrappers = toWrappers(messages);
        List<ComparableMessageWrapper> sorted = new ArrayList<>(wrappers);
        Collections.sort(sorted);
        assertEquals(msg, sorted, wrappers);
    }

    // A wrapper for Message that lets us extract a sequence number from its
    // bytes and gives us a string representation that's helpful in error messages
    static class ComparableMessageWrapper implements Comparable<ComparableMessageWrapper> {

        final Message message;

        public ComparableMessageWrapper(Message message) {
            this.message = message;
        }

        long messageId() {
            return idForMessage(message);
        }

        public String toString() {
            return messageId() + ":" + message.getSubject();
        }

        public boolean equals(Object o) {
            if (o == this) {
                return true;
            } else if (o == null) {
                return false;
            } else if (o instanceof ComparableMessageWrapper) {
                ComparableMessageWrapper c = (ComparableMessageWrapper) o;
                if (c.messageId() == messageId()) {
                    return c.message.getSubject().equals(c.message.getSubject());
                }
            }
            return false;
        }

        public int hashCode() {
            Long id = messageId();
            return id.hashCode();
        }

        @Override
        public int compareTo(ComparableMessageWrapper o) {
            Long a = messageId();
            Long b = o.messageId();
            return a.compareTo(b);
        }
    }

    static long idForMessage(Message message) {
        return ByteBuffer.wrap(message.getData()).asLongBuffer().get();
    }

    static final byte[] bytes() {
        byte[] result = new byte[8];
        ByteBuffer.wrap(result).asLongBuffer().put(MESSAGE_IDS.incrementAndGet());
        return result;
    }

    static final class SubscriberKey {

        final long id = IDS.incrementAndGet();
        final String name;

        public SubscriberKey(String name) {
            this.name = name;
        }
    }

    // Like a CountDownLatch but can count down N items at once, used
    // to track when all messages are received
    static class MultiCountDownLatch {

        private static final class Sync extends AbstractQueuedSynchronizer {

            Sync(int count) {
                setState(count);
            }

            int getCount() {
                return getState();
            }

            void reset(int count) {
                setState(count);
            }

            @Override
            protected int tryAcquireShared(int acquires) {
                return (getState() == 0) ? 1 : -1;
            }

            @Override
            protected boolean tryReleaseShared(int releases) {
                // Decrement count; signal when transition to zero
                for (;;) {
                    int c = getState();
                    if (c == 0) {
                        return false;
                    }
                    int nextc = c - releases;
                    if (compareAndSetState(c, nextc)) {
                        return nextc == 0;
                    }
                }
            }
        }

        private final Sync sync;

        public MultiCountDownLatch(int count) {
            this.sync = new Sync(count);
        }

        public void await() throws InterruptedException {
            sync.acquireSharedInterruptibly(1);
        }

        public boolean await(long timeout, TimeUnit unit)
                throws InterruptedException {
            return sync.tryAcquireSharedNanos(1, unit.toNanos(timeout));
        }

        public void countDown(int qty) {
            sync.releaseShared(qty);
        }

        public long getCount() {
            return sync.getCount();
        }
    }
}
