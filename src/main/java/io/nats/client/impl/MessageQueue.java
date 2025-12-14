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

import io.nats.client.NatsSystemClock;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static io.nats.client.support.NatsConstants.*;

class MessageQueue {
    protected static final int STOPPED = 0;
    protected static final int RUNNING = 1;
    protected static final int DRAINING = 2;
    protected static final long MIN_OFFER_TIMEOUT_NANOS = 100 * NANOS_PER_MILLI;

    protected final AtomicLong length;
    protected final AtomicLong sizeInBytes;
    protected final AtomicInteger running;
    protected final boolean singleReaderMode;
    protected final LinkedBlockingQueue<NatsMessage> queue;
    protected final Lock editLock;
    protected final int maxMessagesInOutgoingQueue;
    protected final boolean discardWhenFull;
    protected final long offerLockNanos;
    protected final long offerTimeoutNanos;
    protected final Duration requestCleanupInterval;

    // Used for POISON_PILL and NatsConnectionWriter.END_RECONNECT
    static class TerminalMessage extends NatsMessage {
        public TerminalMessage(String subject) {
            super(subject, null, EMPTY_BODY);
        }

        @Override
        boolean isFilterOnStop() {
            return true; // always filtered out
        }
    }

    // A simple == is used to resolve if any message is exactly the static pill object in question
    // ----------
    // 1. Poison pill is a graphic, but common term for an item that breaks loops or stop something.
    // In this class the poison pill is used to break out of timed waits on the blocking queue.
    protected static final NatsMessage POISON_PILL = new TerminalMessage("_poison");

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
     * @param requestCleanupInterval is used to figure the offer timeout
     */
    MessageQueue(boolean singleReaderMode, int maxMessagesInOutgoingQueue, boolean discardWhenFull, Duration requestCleanupInterval) {
        this(singleReaderMode, maxMessagesInOutgoingQueue, discardWhenFull, requestCleanupInterval, null);
    }

    MessageQueue(boolean singleReaderMode, int maxMessagesInOutgoingQueue, boolean discardWhenFull, Duration requestCleanupInterval, MessageQueue source) {
        this.maxMessagesInOutgoingQueue = maxMessagesInOutgoingQueue;
        this.queue = maxMessagesInOutgoingQueue > 0 ? new LinkedBlockingQueue<>(maxMessagesInOutgoingQueue) : new LinkedBlockingQueue<>();
        this.discardWhenFull = discardWhenFull;
        this.running = new AtomicInteger(RUNNING);
        sizeInBytes = new AtomicLong(0);
        length = new AtomicLong(0);
        this.offerLockNanos = requestCleanupInterval.toNanos();
        this.offerTimeoutNanos = Math.max(MIN_OFFER_TIMEOUT_NANOS, requestCleanupInterval.toMillis() * NANOS_PER_MILLI * 95 / 100) ;

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
            target.length.set(length.getAndSet(0));
            target.sizeInBytes.set(sizeInBytes.getAndSet(0));
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
        // poison pill  and any other "mark" messages are not included in the length count, or the size
        return this.running.get() == DRAINING && length.get() == 0;
    }

    boolean push(NatsMessage msg) {
        return push(msg, false);
    }

    boolean push(NatsMessage msg, boolean internal) {
        try {
            long startNanos = NatsSystemClock.nanoTime();
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
                The 4750 is 95% of that time. The MIN_OFFER_TIMEOUT_NANOS 100 ms minimum is arbitrary.
             */
            if (editLock.tryLock(offerLockNanos, TimeUnit.NANOSECONDS)) {
                try {
                    // offer with no timeout returns true if the queue was not full
                    if (!internal && discardWhenFull) {
                        if (queue.offer(msg)) {
                            messageAdded(msg);
                            return true;
                        }
                        return false;
                    }

                    long timeoutNanosLeft = Math.max(MIN_OFFER_TIMEOUT_NANOS, offerTimeoutNanos - (NatsSystemClock.nanoTime() - startNanos));

                    // offer with timeout
                    if (!queue.offer(msg, timeoutNanosLeft, TimeUnit.NANOSECONDS)) {
                        throw new IllegalStateException(OUTPUT_QUEUE_IS_FULL + queue.size());
                    }
                    messageAdded(msg);
                    return true;

                }
                finally {
                    editLock.unlock();
                }
            }
            else {
                throw new IllegalStateException(OUTPUT_QUEUE_BUSY + queue.size());
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * poisoning the queue puts the known poison pill into the queue, forcing any waiting code to stop
     * waiting and return.
     * This is done outside of push so it is not counted in length or size
     * offer is used instead of add since we don't care if it fails because it was full
     */
    void poisonTheQueue() {
        queue.offer(POISON_PILL);
    }

    /**
     * Marking the queue, like POISON, is a message we don't want to count.
     * Intended to only be used with an unbounded queue. Use at your own risk.
     * @param msg the mark
     */
    void queueTerminalMessage(TerminalMessage msg) {
        queue.offer(msg);
    }

    private NatsMessage _poll(Duration timeout) throws InterruptedException {
        NatsMessage msg = null;

        if (timeout == null || this.isDraining()) { // try immediately
            msg = this.queue.poll();
        } 
        else {
            long nanos = timeout.toNanos();
            if (nanos != 0) {
                msg = this.queue.poll(nanos, TimeUnit.NANOSECONDS);
            } 
            else {
                // A value of 0 means wait forever
                // We will loop and wait for a LONG time
                // if told to suspend/drain the poison pill will break this loop
                while (this.isRunning()) {
                    msg = this.queue.poll(100, TimeUnit.DAYS);
                    if (msg != null) break;
                }
            }
        }

        return msg == null || msg == POISON_PILL ? null : msg;
    }

    NatsMessage pop(Duration timeout) throws InterruptedException {
        if (!this.isRunning()) {
            return null;
        }

        NatsMessage msg = _poll(timeout);

        if (msg == null) {
            return null;
        }

        sizeInBytes.getAndAdd(-msg.getSizeInBytes());
        length.decrementAndGet();

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
    NatsMessage accumulate(long maxBytesToAccumulate, long maxMessagesToAccumulate, Duration timeout)
        throws InterruptedException {

        if (!this.singleReaderMode) {
            throw new IllegalStateException("Accumulate is only supported in single reader mode.");
        }

        if (!this.isRunning()) {
            return null;
        }

        // _poll returns null if no messages or was a POISON_PILL
        NatsMessage msg = _poll(timeout);
        if (msg == null) {
            return null;
        }
        // TerminalMessage is specifically a termination. Claude said instanceof is fast
        // We know terminal messages are not counted
        if (msg instanceof TerminalMessage) {
            return msg;
        }

        long accumulated = 1;
        long accumulatedSize = msg.getSizeInBytes();

        if (maxMessagesToAccumulate <= 1 || accumulatedSize >= maxBytesToAccumulate) {
            length.decrementAndGet();
            sizeInBytes.addAndGet(-accumulatedSize);
            return msg;
        }

        NatsMessage cursor = msg;

        while (true) {
            NatsMessage next = this.queue.peek();
            if (next == null) {
                break;
            }
            if (next instanceof TerminalMessage) {
                // get the message out of the queue b/c we only peeked
                // POISON_PILL does not get added to the cursor.next chain
                // other Terminal Messages always do.
                this.queue.poll();
                if (next != POISON_PILL) {
                    cursor.next = next;
                }
                break;
            }
            long size = next.getSizeInBytes();
            if (maxBytesToAccumulate < 0 || (accumulatedSize + size) < maxBytesToAccumulate) { // keep going
                accumulated++;
                accumulatedSize += size;

                this.queue.poll(); // get the message out of the queue b/c we only peeked
                cursor.next = next;
                if (next.flushImmediatelyAfterPublish) {
                    // if we are going to flush, then don't accumulate more
                    break;
                }
                if (accumulated == maxMessagesToAccumulate) {
                    break;
                }
                cursor = cursor.next;
            }
            else { // One more is too far
                break;
            }
        }

        length.addAndGet(-accumulated);
        sizeInBytes.addAndGet(-accumulatedSize);

        return msg;
    }

    private void messageAdded(NatsMessage msg) {
        // this must only be called with counted messages
        sizeInBytes.getAndAdd(msg.getSizeInBytes());
        length.incrementAndGet();
    }

    // Returns a message or null
    NatsMessage popNow() throws InterruptedException {
        return pop(null);
    }

    long length() {
        return length.get();
    }

    long sizeInBytes() {
        return sizeInBytes.get();
    }

    void filterOnStop() {
        editLock.lock();
        try {
            if (this.isRunning()) {
                throw new IllegalStateException("Filter is only supported when the queue is paused");
            }
            ArrayList<NatsMessage> newQueue = new ArrayList<>();
            NatsMessage cursor = this.queue.poll();
            while (cursor != null) {
                if (cursor.isFilterOnStop()) {
                    // 1. do not add it to the new queue
                    // 2. reduce the count if it's not a terminal message
                    if (!(cursor instanceof TerminalMessage)) {
                        sizeInBytes.addAndGet(-cursor.getSizeInBytes());
                        length.decrementAndGet();
                    }
                }
                else {
                    newQueue.add(cursor);
                }
                cursor = this.queue.poll();
            }
            this.queue.addAll(newQueue);
        } finally {
            editLock.unlock();
        }
    }

    void clear() {
        editLock.lock();
        try {
            this.queue.clear();
            length.set(0);
            sizeInBytes.set(0);
        } finally {
            editLock.unlock();
        }
    }
}
