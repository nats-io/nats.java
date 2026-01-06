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

import static io.nats.client.Options.MINIMUM_WRITE_QUEUE_PUSH_TIMEOUT;
import static io.nats.client.impl.MarkerMessage.POISON_PILL;
import static io.nats.client.support.NatsConstants.OUTPUT_QUEUE_BUSY;
import static io.nats.client.support.NatsConstants.OUTPUT_QUEUE_IS_FULL;

class WriterMessageQueue extends MessageQueueBase {
    protected static final long MIN_PUSH_TIMEOUT_NANOS = MINIMUM_WRITE_QUEUE_PUSH_TIMEOUT.toNanos();

    protected final int maxMessagesInOutgoingQueue;
    protected final boolean discardWhenFull;
    protected final Lock editLock;
    protected final long pushTimeoutNanos;

    WriterMessageQueue(Duration pushTimeout) {
        this(-1, false, pushTimeout);
    }

    WriterMessageQueue(int maxMessagesInOutgoingQueue, boolean discardWhenFull, Duration pushTimeout) {
        super(maxMessagesInOutgoingQueue);
        this.maxMessagesInOutgoingQueue = queueCapacity;
        this.discardWhenFull = discardWhenFull;
        this.pushTimeoutNanos = Math.max(MIN_PUSH_TIMEOUT_NANOS, pushTimeout.toNanos());
        this.editLock = new ReentrantLock();
    }

    boolean push(NatsMessage msg) {
        return push(msg, false);
    }

    boolean push(NatsMessage msg, boolean internal) {
        try {
            long startNanos = NatsSystemClock.nanoTime();
            if (editLock.tryLock(pushTimeoutNanos, TimeUnit.NANOSECONDS)) {
                try {
                    long timeoutNanosLeft = Math.max(
                        MIN_PUSH_TIMEOUT_NANOS,
                        pushTimeoutNanos - (NatsSystemClock.nanoTime() - startNanos)
                    );
                   if (queue.offer(msg, timeoutNanosLeft, TimeUnit.NANOSECONDS)) {
                        sizeInBytes.getAndAdd(msg.getSizeInBytes());
                        length.incrementAndGet();
                        return true;
                    }
                    // internal or !discardWhenFull throws instead of returns
                    if (!internal && discardWhenFull) {
                        return false;
                    }
                    throw new IllegalStateException(OUTPUT_QUEUE_IS_FULL + queue.size());
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
        if (this.isRunning()) {
            throw new IllegalStateException("Filter is only supported when the queue is paused");
        }
        editLock.lock();
        try {
            ArrayList<NatsMessage> temp = new ArrayList<>();
            queue.drainTo(temp);
            for (NatsMessage cursor : temp) {
                if (cursor.isFilterOnStop()) {
                    sizeInBytes.addAndGet(-cursor.getSizeInBytes());
                    length.decrementAndGet();
                }
                else {
                    queue.offer(cursor);
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
