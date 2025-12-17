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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static io.nats.client.impl.MarkerMessage.POISON_PILL;
import static io.nats.client.support.NatsConstants.*;

class WriterMessageQueue {
    private static final long MIN_OFFER_TIMEOUT_NANOS = 100 * NANOS_PER_MILLI;

    private final boolean discardWhenFull;

    private final long offerLockNanos;
    private final long offerTimeoutNanos;

    private final AtomicBoolean running;
    private final ReentrantLock editLock;

    private final LinkedBlockingQueue<NatsMessage> queue;
    private final AtomicLong length;
    private final AtomicLong sizeInBytes;

    /**
     * If maxMessagesInOutgoingQueue is set to a number of messages, the publish command will block, which provides
     * backpressure on a publisher if the writer is slow to push things onto the network. Publishers use the value of Options.getMaxMessagesInOutgoingQueue().
     *
     * @param maxMessagesInOutgoingQueue sets a limit on the size of the underlying queue
     * @param discardWhenFull            allows to discard messages when the underlying queue is full
     * @param requestCleanupInterval     is used to figure the offer timeout
     */
    WriterMessageQueue(int maxMessagesInOutgoingQueue, boolean discardWhenFull, Duration requestCleanupInterval) {
         this.discardWhenFull = discardWhenFull;

        offerLockNanos = requestCleanupInterval.toNanos();
        offerTimeoutNanos = Math.max(MIN_OFFER_TIMEOUT_NANOS, requestCleanupInterval.toMillis() * NANOS_PER_MILLI * 95 / 100) ;

        running = new AtomicBoolean(true);
        editLock = new ReentrantLock();

        queue = maxMessagesInOutgoingQueue > 0 ? new LinkedBlockingQueue<>(maxMessagesInOutgoingQueue) : new LinkedBlockingQueue<>();
        length = new AtomicLong(0);
        sizeInBytes = new AtomicLong(0);
    }

    boolean isRunning() {
        return running.get();
    }

    void resume() {
        running.set(true);
    }

    boolean push(NatsMessage msg) {
        return push(msg, false);
    }
    
    boolean push(NatsMessage msg, boolean internal) {
        try {
            /*
                The editLock is used to address a Head-Of-Line blocking problem and to
                also ensure that edits happen in order. See filterOnStop and clear

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
            long startNanos = NatsSystemClock.nanoTime();
            if (editLock.tryLock(offerLockNanos, TimeUnit.NANOSECONDS)) {
                try {
                    if (!internal && discardWhenFull) {
                        if (queue.offer(msg)) { // offer with no timeout returns true if the queue was not full
                            length.incrementAndGet();
                            sizeInBytes.addAndGet(msg.getSizeInBytes());
                            return true;
                        }
                        return false;
                    }

                    long timeoutNanosLeft = Math.max(MIN_OFFER_TIMEOUT_NANOS, offerTimeoutNanos - (NatsSystemClock.nanoTime() - startNanos));
                    // offer with timeout
                    if (queue.offer(msg, timeoutNanosLeft, TimeUnit.NANOSECONDS)) {
                        length.incrementAndGet();
                        sizeInBytes.addAndGet(msg.getSizeInBytes());
                        return true;
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
     * poisoning the queue puts the known poison pill into the queue,
     * forcing any waiting code to stop waiting and return.
     * This is done without calling push since it is not counted in length or size
     * offer is used instead of add since we don't care if it fails if it was full
     */
    void poisonTheQueue() {
        queue.offer(POISON_PILL);
    }

    /**
     * Queue a message we don't want to count.
     * offer is used instead of add since it should never fail,
     * because it is intended to only be used with an unbounded queue
     * Use carefully.
     * @param msg the message
     */
    void queueMarkerMessage(MarkerMessage msg) {
        queue.offer(msg);
    }

    private NatsMessage _poll(Duration timeout) throws InterruptedException {
        NatsMessage msg = null;

        if (timeout == null) { // try immediately
            msg = queue.poll();
        } 
        else {
            long nanos = timeout.toNanos();
            if (nanos != 0) {
                msg = queue.poll(nanos, TimeUnit.NANOSECONDS);
            } 
            else {
                // A value of 0 means wait forever
                // We will loop and wait for a LONG time
                // if told to pause the poison pill will break this loop
                while (isRunning()) {
                    msg = queue.poll(100, TimeUnit.DAYS);
                    if (msg != null) break;
                }
            }
        }

        return msg == null || msg == POISON_PILL ? null : msg;
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

    long size() {
        return queue.size();
    }

    long length() {
        return length.get();
    }

    long sizeInBytes() {
        return sizeInBytes.get();
    }


    void pause() {
        if (running.compareAndSet(true, false)) {
            poisonTheQueue();
            // the editLock is used here to ensure the filter happens before any other messages get pushed.
            editLock.lock();
            try {
                ArrayList<NatsMessage> tempQueue = new ArrayList<>();
                // I realize this is a transfer of a queue, but polling "read" locks
                // the queue every time, whereas this uses one read lock.
                queue.drainTo(tempQueue);
                long left = length.get();
                long bytesLeft = sizeInBytes.get();
                for (NatsMessage msg : tempQueue) {
                    if (msg instanceof ProtocolMessage) {
                        // do not add it to the new queue and track the count
                        left--;
                        bytesLeft -= msg.getSizeInBytes();
                    }
                    else {
                        // The queue was cleared by drainTo, so was fresh
                        // to add back in message wanted to be kept.
                        queue.offer(msg);
                    }
                }
                length.set(left);
                sizeInBytes.set(bytesLeft);
            }
            finally {
                editLock.unlock();
            }
        }
    }

    void clear() {
        editLock.lock();
        try {
            queue.clear();
            length.set(0);
            sizeInBytes.set(0);
        }
        finally {
            editLock.unlock();
        }
    }
}
