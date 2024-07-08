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

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import static io.nats.client.support.NatsConstants.EMPTY_BODY;
import static io.nats.client.support.NatsConstants.OUTPUT_QUEUE_IS_FULL;

class MessageQueue {
    protected static final int STOPPED = 0;
    protected static final int RUNNING = 1;
    protected static final int DRAINING = 2;
    protected static final String POISON = "_poison";

    protected final AtomicLong length;
    protected final AtomicLong sizeInBytes;
    protected final AtomicInteger running;
    protected final boolean singleReaderMode;
    protected final LinkedBlockingQueue<NatsMessage> queue;
    protected final Lock editLock;
    protected final int maxMessagesInOutgoingQueue;
    protected final boolean discardWhenFull;
    protected final long offerLockMillis;
    protected final long offerTimeoutMillis;
    protected final Duration requestCleanupInterval;

    // Poison pill is a graphic, but common term for an item that breaks loops or stop something.
    // In this class the poison pill is used to break out of timed waits on the blocking queue.
    // A simple == is used to check if any message in the queue is this message.
    // /\ /\ /\ /\ which is why it is now a static. It's just a marker anyway.
    protected static final NatsMessage POISON_PILL = new NatsMessage(POISON, null, EMPTY_BODY);

    MessageQueue(boolean singleReaderMode, Duration requestCleanupInterval) {
        this(singleReaderMode, -1, false, requestCleanupInterval, null);
    }

    MessageQueue(boolean singleReaderMode, Duration requestCleanupInterval, MessageQueue source) {
        this(singleReaderMode, -1, false, requestCleanupInterval, source);
    }

    /**
     * If publishHighwaterMark is set to 0 the underlying queue can grow forever (or until the max size of a linked blocking queue that is).
     * A value of 0 is used by readers to prevent the read thread from blocking.
     * If set to a number of messages, the publish command will block, which provides
     * backpressure on a publisher if the writer is slow to push things onto the network. Publishers use the value of Options.getMaxMessagesInOutgoingQueue().
     * @param singleReaderMode allows the use of "accumulate"
     * @param maxMessagesInOutgoingQueue sets a limit on the size of the underlying queue
     * @param discardWhenFull allows to discard messages when the underlying queue is full
     * @param requestCleanupInterval is used to figure the offerTimeoutMillis
     */
    MessageQueue(boolean singleReaderMode, int maxMessagesInOutgoingQueue, boolean discardWhenFull, Duration requestCleanupInterval) {
        this(singleReaderMode, maxMessagesInOutgoingQueue, discardWhenFull, requestCleanupInterval, null);
    }

    MessageQueue(boolean singleReaderMode, int maxMessagesInOutgoingQueue, boolean discardWhenFull, Duration requestCleanupInterval, MessageQueue source) {
        this.maxMessagesInOutgoingQueue = maxMessagesInOutgoingQueue;
        this.queue = maxMessagesInOutgoingQueue > 0 ? new LinkedBlockingQueue<>(maxMessagesInOutgoingQueue) : new LinkedBlockingQueue<>();
        this.discardWhenFull = discardWhenFull;
        this.running = new AtomicInteger(RUNNING);
        this.sizeInBytes = new AtomicLong(0);
        this.length = new AtomicLong(0);
        this.offerLockMillis = requestCleanupInterval.toMillis();
        this.offerTimeoutMillis = Math.max(1, requestCleanupInterval.toMillis() * 95 / 100);

        editLock = new ReentrantLock();
        
        this.singleReaderMode = singleReaderMode;
        this.requestCleanupInterval = requestCleanupInterval;

        if (source != null) {
            source.drainTo(this);
        }
    }

    void drainTo(MessageQueue target) {
        editLock.lock();
        try {
            queue.drainTo(target.queue);
            target.length.set(queue.size());
        } finally {
            editLock.unlock();
        }
    }

    boolean isSingleReaderMode() {
        return singleReaderMode;
    }

    boolean isRunning() {
        return this.running.get() != STOPPED;
    }

    boolean isDraining() {
        return this.running.get() == DRAINING;
    }

    void pause() {
        this.running.set(STOPPED);
        this.poisonTheQueue();
    }

    void resume() {
        this.running.set(RUNNING);
    }

    void drain() {
        this.running.set(DRAINING);
        this.poisonTheQueue();
    }

    boolean isDrained() {
        // poison pill is not included in the length count, or the size
        return this.running.get() == DRAINING && this.length() == 0;
    }

    boolean push(NatsMessage msg) {
        return push(msg, false);
    }

    boolean push(NatsMessage msg, boolean internal) {
        long start = System.currentTimeMillis();
        try {
            /*
                This was essentially a Head-Of-Line blocking problem.

                So the crux of the problem was that many threads were waiting to push a message to the queue.
                They all waited for the lock and once they had the lock they waited 5 seconds (4750 millis actually)
                only to find out the queue was full. They released the lock, so then another thread acquired the lock,
                and waited 5 seconds. So instead of being parallel, all these threads had to wait in line
                200 * 4750 = 15.8 minutes

                So what I did was try to acquire the lock but only wait 5 seconds.
                If I could not acquire the lock, then I assumed that this means that we are in this exact situation,
                another thread can't add b/c the queue is full, and so there is no point in even trying, so just throw the queue full exception.

                If I did acquire the lock, I deducted the time spent waiting for the lock from the time allowed to try to add.
                I took the max of that or 100 millis to try to add to the queue.
                This ensures that the max total time each thread can take is 5100 millis in parallel.

                Notes: The 5 seconds and the 4750 seconds is derived from the Options requestCleanupInterval, which defaults to 5 seconds and can be modified.
                The 4750 is 95% of that time. The 100 ms minimum is arbitrary.
             */
            if (!editLock.tryLock(offerLockMillis, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException(OUTPUT_QUEUE_IS_FULL + queue.size());
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        try {
            if (!internal && this.discardWhenFull) {
                return this.queue.offer(msg);
            }

            long timeoutLeft = Math.max(100, offerTimeoutMillis - (System.currentTimeMillis() - start));

            if (!this.queue.offer(msg, timeoutLeft, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException(OUTPUT_QUEUE_IS_FULL + queue.size());
            }
            this.sizeInBytes.getAndAdd(msg.getSizeInBytes());
            this.length.incrementAndGet();
            return true;
        } catch (InterruptedException ie) {
            return false;
        } finally {
            editLock.unlock();
        }
    }

    /**
     * poisoning the queue puts the known poison pill into the queue, forcing any waiting code to stop
     * waiting and return.
     */
    void poisonTheQueue() {
        try {
            this.queue.add(POISON_PILL);
        } catch (IllegalStateException ie) { // queue was full, so we don't really need poison pill
            // ok to ignore this
        }
    }

    NatsMessage poll(Duration timeout) throws InterruptedException {
        NatsMessage msg = null;
        
        if (timeout == null || this.isDraining()) { // try immediately
            msg = this.queue.poll();
        } else {
            long nanos = timeout.toNanos();

            if (nanos != 0) {
                msg = this.queue.poll(nanos, TimeUnit.NANOSECONDS);
            } else {
                // A value of 0 means wait forever
                // We will loop and wait for a LONG time
                // if told to suspend/drain the poison pill will break this loop
                while (this.isRunning()) {
                    msg = this.queue.poll(100, TimeUnit.DAYS);
                    if (msg != null) break;
                }
            }
        }

        return msg == null || isPoison(msg) ? null : msg;
    }

    private boolean isPoison(Message msg) {
        return msg == POISON_PILL;
    }

    NatsMessage pop(Duration timeout) throws InterruptedException {
        if (!this.isRunning()) {
            return null;
        }

        NatsMessage msg = this.poll(timeout);

        if (msg == null) {
            return null;
        }

        this.sizeInBytes.getAndAdd(-msg.getSizeInBytes());
        this.length.decrementAndGet();

        return msg;
    }
    
    // Waits up to the timeout to try to accumulate multiple messages
    // Use the next field to read the entire set accumulated.
    // maxSize and maxMessages are both checked and if either is exceeded
    // the method returns.
    //
    // A timeout of 0 will wait forever (or until the queue is stopped/drained)
    //
    // Only works in single reader mode, because we want to maintain order.
    // accumulate reads off the concurrent queue one at a time, so if multiple
    // readers are present, you could get out of order message delivery.
    NatsMessage accumulate(long maxSize, long maxMessages, Duration timeout)
            throws InterruptedException {

        if (!this.singleReaderMode) {
            throw new IllegalStateException("Accumulate is only supported in single reader mode.");
        }

        if (!this.isRunning()) {
            return null;
        }

        NatsMessage msg = this.poll(timeout);

        if (msg == null) {
            return null;
        }

        long size = msg.getSizeInBytes();

        if (maxMessages <= 1 || size >= maxSize) {
            this.sizeInBytes.addAndGet(-size);
            this.length.decrementAndGet();
            return msg;
        }

        long count = 1;
        NatsMessage cursor = msg;

        while (cursor != null) {
            NatsMessage next = this.queue.peek();
            if (next != null && !isPoison(next)) {
                long s = next.getSizeInBytes();

                if (maxSize<0 || (size + s) < maxSize) { // keep going
                    size += s;
                    count++;
                    
                    cursor.next = this.queue.poll();
                    cursor = cursor.next;

                    if (count == maxMessages) {
                        break;
                    }
                } else { // One more is too far
                    break;
                }
            } else { // Didn't meet max condition
                break;
            }
        }

        this.sizeInBytes.addAndGet(-size);
        this.length.addAndGet(-count);

        return msg;
    }

    // Returns a message or null
    NatsMessage popNow() throws InterruptedException {
        return pop(null);
    }

    // Just for testing
    long length() {
        return this.length.get();
    }

    long sizeInBytes() {
        return this.sizeInBytes.get();
    }

    void filter(Predicate<NatsMessage> p) {
        editLock.lock();
        try {
            if (this.isRunning()) {
                throw new IllegalStateException("Filter is only supported when the queue is paused");
            }
            ArrayList<NatsMessage> newQueue = new ArrayList<>();
            NatsMessage cursor = this.queue.poll();
            while (cursor != null) {
                if (!p.test(cursor)) {
                    newQueue.add(cursor);
                } else {
                    this.sizeInBytes.addAndGet(-cursor.getSizeInBytes());
                    this.length.decrementAndGet();
                }
                cursor = this.queue.poll();
            }
            this.queue.addAll(newQueue);
        } finally {    
            editLock.unlock();
        }
    }
}
