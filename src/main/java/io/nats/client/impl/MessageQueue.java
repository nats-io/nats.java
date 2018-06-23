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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

class MessageQueue {
    // Dev notes (sasbury)
    // I tried this with semaphore and ConcurrentLinkedDeque.
    // Neither version was an improvement on this version and
    // both had some issues, including spin locking with a very short
    // thread sleep. The lock/condition is currently a performance
    // bottleneck, that should be continuously investigated.
    private long length;
    private long sizeInBytes;
    private NatsMessage head;
    private NatsMessage tail;
    private ReentrantLock lock;
    private Condition condition;
    private boolean interrupted;

    MessageQueue() {
        this.lock = new ReentrantLock(false);
        this.condition = lock.newCondition();
        this.interrupted = false;
        this.length = 0;
        this.sizeInBytes = 0;
    }

    void interrupt() {
        this.lock.lock();
        try {
            this.interrupted = true;
            this.condition.signalAll();
        } finally {
            this.lock.unlock();
        }
    }

    void reset() {
        this.lock.lock();
        try {
            this.interrupted = false;
            this.condition.signalAll();
        } finally {
            this.lock.unlock();
        }
    }

    void push(NatsMessage msg) {
        lock.lock();
        try {
            if (this.length == 0) {
                this.head = this.tail = msg;
            } else {
                this.head.next = msg;
                this.head = msg;
            }
            this.length = this.length + 1;
            this.sizeInBytes = this.sizeInBytes + msg.getSizeInBytes();
        } finally {
            condition.signalAll();
            lock.unlock();
        }
    }

    // Returns a message or waits up to the timeout, returns null if timed out
    // A timeout of 0 will wait indefinitely
    NatsMessage pop(Duration timeout) throws InterruptedException {
        NatsMessage retVal = null;
        long now = System.nanoTime();
        long start = now;
        long timeoutNanos = (timeout != null) ? timeout.toNanos() : -1;

        lock.lock();
        try {
            if (timeoutNanos >= 0) {
                while ((this.length == 0) && !this.interrupted) {
                    if (timeoutNanos > 0) { // If it is 0, keep it, otherwise reduce for time elapsed
                        now = System.nanoTime();
                        timeoutNanos = timeoutNanos - (now - start);
                        start = now;

                        if (timeoutNanos <= 0) { // just in case we hit it exactly
                            break;
                        }
                    }

                    condition.await(timeoutNanos, TimeUnit.NANOSECONDS);
                }
            }

            if (this.interrupted || (this.length == 0)) {
                return null;
            }

            retVal = this.tail;

            if (this.head == this.tail) {
                this.head = this.tail = null;
            } else {
                this.tail = retVal.next;
            }

            retVal.next = null;
            this.length--;
            this.sizeInBytes = this.sizeInBytes - retVal.getSizeInBytes();
        } finally {
            condition.signalAll();
            lock.unlock();
        }

        return retVal;
    }

    // Waits up to the timeout to try to accumulate multiple messages
    // To avoid allocations, the "last" message from the queue is returned
    // Use the prev field to read the entire set accumulated.
    // maxSize and maxMessages are both checked and if either is exceeded
    // the method returns.
    //
    // A timeout of 0 will wait indefinitely
    NatsMessage accumulate(long maxSize, long maxMessages, Duration timeout)
            throws InterruptedException {
        NatsMessage oldestMessage = null;
        NatsMessage newestMessage = null; // first, but these should be read in reverse order, with this one last
        long now = System.nanoTime();
        long start = now;
        long timeoutNanos = (timeout != null) ? timeout.toNanos() : -1;

        lock.lock();
        try {
            if (timeoutNanos >= 0) {
                while ((this.length == 0) && !this.interrupted) {
                    if (timeoutNanos > 0) { // If it is 0, keep it as zero, otherwise reduce based on time
                        now = System.nanoTime();
                        timeoutNanos = timeoutNanos - (now - start);
                        start = now;

                        if (timeoutNanos <= 0) { // just in case we hit it exactly
                            break;
                        }
                    }

                    condition.await(timeoutNanos, TimeUnit.NANOSECONDS);
                }
            }

            if (this.interrupted || (this.length == 0)) {
                return null;
            }

            oldestMessage = this.tail;
            newestMessage = findMax(oldestMessage, maxSize, maxMessages);

            // Take them all, if we didn't meet max condition
            if (newestMessage == null) {
                newestMessage = this.head;
            }

            // Update the linked list
            if (this.head == this.tail || this.head == newestMessage) {
                this.head = this.tail = null;
            } else {
                this.tail = newestMessage.next;
            }

            newestMessage.next = null;

            // Update the length
            NatsMessage cursor = oldestMessage;
            while (cursor != null) {
                this.length--;
                this.sizeInBytes = this.sizeInBytes - cursor.getSizeInBytes();
                cursor = cursor.next;
            }
        } finally {
            condition.signalAll();
            lock.unlock();
        }

        return oldestMessage;
    }

    // Find the last message that satisfies the max condition. Returns the start
    // if the next message would overrun max size or max messages. Returns null
    // if there are not enough messages to meet the max condition.
    private NatsMessage findMax(NatsMessage start, long maxSize, long maxMessages) {

        if (start == null) {
            return null;
        }

        NatsMessage cursor = start;
        long size = start.getSizeInBytes();
        long count = 1;

        while (cursor != null) {
            if (cursor.next != null) {
                long s = cursor.next.getSizeInBytes();

                if ((size + s) < maxSize) { // keep going
                    cursor = cursor.next;
                    size += s;
                    count++;

                    if (count == maxMessages) {
                        break;
                    }
                } else { // One more is too far, return the cursor
                    break;
                }
            } else { // Didn't meet max condition
                cursor = null;
                break;
            }
        }

        return cursor;
    }

    // Returns a message or null
    NatsMessage popNow() throws InterruptedException {
        return pop(null);
    }

    // Not thread safe, just for testing
    long length() {
        return this.length;
    }

    long sizeInBytes() {
        return this.sizeInBytes;
    }

    void filter(Predicate<NatsMessage> p) {
        lock.lock();
        try {
            NatsMessage cursor = tail;
            NatsMessage previous = null;

            while (cursor != null) {
                if (p.test(cursor)) {
                    NatsMessage toRemove = cursor;

                    if (toRemove == this.head) {
                        this.head = previous;

                        if (previous != null) {
                            previous.next = null;
                        }
                    } else if (toRemove == this.tail) {
                        this.tail = toRemove.next;
                    } else if (previous != null) {
                        previous.next = toRemove.next;
                    }

                    toRemove.next = null;
                    this.length--;

                    previous = cursor;
                    cursor = cursor.next;
                } else {
                    previous = cursor;
                    cursor = cursor.next;
                }
            }
        } finally {
            condition.signalAll();
            lock.unlock();
        }
    }
}