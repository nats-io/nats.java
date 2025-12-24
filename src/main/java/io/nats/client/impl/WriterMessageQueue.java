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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static io.nats.client.impl.MarkerMessage.POISON_PILL;
import static io.nats.client.support.NatsConstants.*;

class WriterMessageQueue extends MessageQueueBase {
    protected static final long MIN_OFFER_TIMEOUT_NANOS = 100 * NANOS_PER_MILLI;

    protected final int maxMessagesInOutgoingQueue;
    protected final boolean discardWhenFull;
    protected final Lock editLock;
    protected final long offerLockNanos;
    protected final long offerTimeoutNanos;
    protected final Duration offerLockWait;

    WriterMessageQueue(int maxMessagesInOutgoingQueue, boolean discardWhenFull, Duration offerLockWait) {
        super(maxMessagesInOutgoingQueue);
        this.maxMessagesInOutgoingQueue = queueCapacity;
        this.discardWhenFull = discardWhenFull;
        this.offerLockNanos = offerLockWait.toNanos();
        this.offerTimeoutNanos = Math.max(MIN_OFFER_TIMEOUT_NANOS, offerLockWait.toMillis() * NANOS_PER_MILLI * 95 / 100) ;

        this.editLock = new ReentrantLock();
        this.offerLockWait = offerLockWait;
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

                Notes: The 5 seconds and the 4750 seconds is derived from the Options offerLockWait, which defaults to 5 seconds and can be modified.
                The 4750 is 95% of that time. The MIN_OFFER_TIMEOUT_NANOS 100 ms minimum is arbitrary.
             */
            if (editLock.tryLock(offerLockNanos, TimeUnit.NANOSECONDS)) {
                try {
                    // offer with no timeout returns true if the queue was not full
                    if (!internal && discardWhenFull) {
                        if (queue.offer(msg)) {
                            sizeInBytes.getAndAdd(msg.getSizeInBytes());
                            length.incrementAndGet();
                            return true;
                        }
                        return false;
                    }

                    long timeoutNanosLeft = Math.max(MIN_OFFER_TIMEOUT_NANOS, offerTimeoutNanos - (NatsSystemClock.nanoTime() - startNanos));

                    // offer with timeout
                    if (!queue.offer(msg, timeoutNanosLeft, TimeUnit.NANOSECONDS)) {
                        throw new IllegalStateException(OUTPUT_QUEUE_IS_FULL + queue.size());
                    }
                    sizeInBytes.getAndAdd(msg.getSizeInBytes());
                    length.incrementAndGet();
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
     * Marking the queue, like POISON, is a message we don't want to count.
     * Intended to only be used with an unbounded queue. Use at your own risk.
     * @param msg the mark
     */
    @SuppressWarnings("SameParameterValue")
    void queueMarkerMessage(MarkerMessage msg) {
        queue.offer(msg);
    }

    // Waits up to the timeout to try to accumulate multiple messages
    // Use the NatsMessage.next field to read the entire set accumulated.
    // maxBytesToAccumulate and maxMessagesToAccumulate are both checked
    // and if either is exceeded the method returns.
    //
    // A timeout of 0 will wait forever (or until the queue is stopped/drained)
    //
    // Only works in writer mode, because we want to maintain order.
    // accumulate reads off the concurrent queue one at a time, so if multiple
    // readers are present, you could get out of order message delivery.
    NatsMessage accumulate(long maxBytesToAccumulate, long maxMessagesToAccumulate, Duration timeout)
        throws InterruptedException {

        if (!isRunning()) {
            return null;
        }

        // _poll returns null if no messages or was a POISON_PILL
        // MarkerMessage is a termination, is not counted, but is returned
        NatsMessage headMessage = _poll(timeout);
        if (headMessage == null || headMessage instanceof MarkerMessage) {
            return headMessage;
        }

        if (maxBytesToAccumulate < 1) {
            maxBytesToAccumulate = Long.MAX_VALUE; // this just makes it easier to loop
        }

        // these will be used to call count() after the loop ends
        long accumulatedMessages = 1;
        long accumulatedSize = headMessage.getSizeInBytes();

        // We need a cursor for the chain of messages
        // and we return the head message
        NatsMessage cursor = headMessage;

        // If the message wants to flushImmediatelyAfterPublish, don't accumulate more
        // If the accumulatedMessages is >= maxMessagesToAccumulate, don't accumulate more
        while (!cursor.flushImmediatelyAfterPublish && accumulatedMessages < maxMessagesToAccumulate) {
            // We are allowed to try more messages. Peek first to see what we are dealing with
            NatsMessage peeked = queue.peek();

            if (peeked == null) {
                break; // no messages in the queue so we are done.
            }

            if (peeked instanceof MarkerMessage) {
                // - Get the message out of the queue b/c we only peeked
                // - POISON_PILL does not get added to the cursor.next chain
                //   but all other MarkerMessages do.
                // - We are done.
                queue.poll();
                if (peeked != POISON_PILL) {
                    cursor.next = peeked;
                }
                break;
            }

            // How big is the message we just peeked at? Will it put us over maxBytesToAccumulate?
            long size = peeked.getSizeInBytes();
            if (accumulatedSize + size > maxBytesToAccumulate) {
                break; // Too many bytes, so we are done.
            }

            // We can add the peeked message to the chain...
            // - Get the message out of the queue b/c we only peeked
            // - Track the message and the bytes for later counting and the while loop
            // - Add the message to the chain
            queue.poll();
            accumulatedMessages++;
            accumulatedSize += size;
            cursor.next = peeked;

            // Move the cursor. It's okay if the while terminates at it's
            // next check, we don't need the cursor outside the loop
            cursor = peeked;
        }

        length.addAndGet(-accumulatedMessages);
        sizeInBytes.addAndGet(-accumulatedSize);
        return headMessage;
    }

    void filter() {
        editLock.lock();
        try {
            if (this.isRunning()) {
                throw new IllegalStateException("Filter is only supported when the queue is paused");
            }
            ArrayList<NatsMessage> temp = new ArrayList<>();
            queue.drainTo(temp);
            for (NatsMessage cursor : temp) {
                if (cursor.isFilterOnStop()) {
                    sizeInBytes.addAndGet(-cursor.getSizeInBytes());
                    length.decrementAndGet();
                }
                else {
                    queue.add(cursor);
                }
            }
        }
        finally {
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
