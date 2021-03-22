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

class MessageQueue {
    protected final static int STOPPED = 0;
    protected final static int RUNNING = 1;
    protected final static int DRAINING = 2;

    protected final AtomicLong length;
    protected final AtomicLong sizeInBytes;
    protected final AtomicInteger running;
    protected final boolean singleThreadedReader;
    protected final LinkedBlockingQueue<NatsMessage> queue;
    protected final Lock filterLock;
    protected final boolean discardWhenFull;

    // Poison pill is a graphic, but common term for an item that breaks loops or stop something.
    // In this class the poisonPill is used to break out of timed waits on the blocking queue.
    // A simple == is used to check if any message in the queue is this message.
    protected final NatsMessage poisonPill;

    /**
     * If publishHighwaterMark is set to 0 the underlying queue can grow forever (or until the max size of a linked blocking queue that is).
     * A value of 0 is used by readers to prevent the read thread from blocking.
     * If set to a number of messages, the publish command will block, which provides
     * backpressure on a publisher if the writer is slow to push things onto the network. Publishers use the value of Options.getMaxMessagesInOutgoingQueue().
     * @param singleReaderMode allows the use of "accumulate"
     * @param publishHighwaterMark sets a limit on the size of the underlying queue
     * @param discardWhenFull allows to discard messages when the underlying queue is full
     */
    MessageQueue(boolean singleReaderMode, int publishHighwaterMark, boolean discardWhenFull) {
        this.queue = publishHighwaterMark > 0 ? new LinkedBlockingQueue<NatsMessage>(publishHighwaterMark) : new LinkedBlockingQueue<NatsMessage>();
        this.discardWhenFull = discardWhenFull;
        this.running = new AtomicInteger(RUNNING);
        this.sizeInBytes = new AtomicLong(0);
        this.length = new AtomicLong(0);

        // The poisonPill is used to stop poll and accumulate when the queue is stopped
        this.poisonPill = new NatsMessage("_poison", null, EMPTY_BODY, false);

        this.filterLock = new ReentrantLock();
        
        this.singleThreadedReader = singleReaderMode;
    }

    MessageQueue(boolean singleReaderMode) {
        this(singleReaderMode, 0);
    }

    MessageQueue(boolean singleReaderMode, int publishHighwaterMark) {
        this(singleReaderMode, publishHighwaterMark, false);
    }

    boolean isSingleReaderMode() {
        return singleThreadedReader;
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

        this.filterLock.lock();
        try {
            // If we aren't running, then we need to obey the filter lock
            // to avoid ordering problems
            if (!internal && this.discardWhenFull) {
                return this.queue.offer(msg);
            }
            if (!this.offer(msg)) {
                throw new IllegalStateException("Output queue is full " + queue.size());
            }
            this.sizeInBytes.getAndAdd(msg.getSizeInBytes());
            this.length.incrementAndGet();
            return true;
        } finally {
            this.filterLock.unlock();
        }
    }

    /**
     * poisoning the queue puts the known poison pill into the queue, forcing any waiting code to stop
     * waiting and return.
     */
    void poisonTheQueue() {
        try {
            this.queue.add(this.poisonPill);
        } catch (IllegalStateException ie) { // queue was full, so we don't really need poison pill
            // ok to ignore this
        }
    }

    boolean offer(NatsMessage msg) {
        try {
            return this.queue.offer(msg, 5, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            return false;
        }
    }

    NatsMessage poll(Duration timeout) throws InterruptedException {
        NatsMessage msg = null;
        
        if (timeout == null || this.isDraining()) { // try immediately
            msg = internalPoll();
        } else {
            long nanos = timeout.toNanos();

            if (nanos != 0) {
                msg = internalPoll(nanos, TimeUnit.NANOSECONDS);
            } else {
                // A value of 0 means wait forever
                // We will loop and wait for a LONG time
                // if told to suspend/drain the poison pill will break this loop
                while (this.isRunning()) {
                    msg = internalPoll(100, TimeUnit.DAYS);
                    if (msg != null) break;
                }
            }
        }

        if (msg == poisonPill) {
            return null;
        }

        return msg;
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

    static class AccumulateResult {
        NatsMessage head;
        int count = 0;
        int size = 0;

        public AccumulateResult(NatsMessage head) {
            this.head = head;
        }
    }

    protected NatsMessage internalPeek() {
        return queue.peek();
    }

    protected NatsMessage internalPoll() {
        return queue.poll();
    }

    protected NatsMessage internalPoll(long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    protected NatsMessage getPeekAsPoll() {
        return queue.poll();
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
        this.filterLock.lock();
        try {
            if (this.isRunning()) {
                throw new IllegalStateException("Filter is only supported when the queue is paused");
            }
            ArrayList<NatsMessage> newQueue = new ArrayList<>();
            NatsMessage cursor = internalPoll();
            while (cursor != null) {
                if (!p.test(cursor)) {
                    newQueue.add(cursor);
                } else {
                    this.sizeInBytes.addAndGet(-cursor.getSizeInBytes());
                    this.length.decrementAndGet();
                }
                cursor = internalPoll();
            }
            this.queue.addAll(newQueue);
        } finally {    
            this.filterLock.unlock();
        }
    }
}