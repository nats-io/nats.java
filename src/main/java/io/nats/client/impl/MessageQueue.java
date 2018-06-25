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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;

class MessageQueue {
    private final AtomicLong length;
    private final AtomicLong sizeInBytes;
    private final AtomicBoolean running;
    private final boolean singleThreadedReader;
    private final ConcurrentLinkedQueue<NatsMessage> queue;
    private final ConcurrentLinkedQueue<Thread> waiters;

    MessageQueue(boolean singleReaderMode) {
        this.queue = new ConcurrentLinkedQueue<>();
        this.running = new AtomicBoolean(true);
        this.sizeInBytes = new AtomicLong(0);
        this.length = new AtomicLong(0);
        
        this.waiters = new ConcurrentLinkedQueue<>();
        this.singleThreadedReader = singleReaderMode;
    }

    boolean isSingleReaderMode() {
        return singleThreadedReader;
    }

    void pause() {
        this.running.set(false);
        signalAll();
    }

    void resume() {
        this.running.set(true);
        signalAll();
    }

    void signalOne() {
        Thread t = waiters.poll();
        if (t != null) {
            LockSupport.unpark(t);
        }
    }

    void signalIfNotEmpty() {
        if (this.length.get() > 0) {
            signalOne();
        }
    }

    void signalAll() {
        Thread t = waiters.poll();
        while(t != null) {
            LockSupport.unpark(t);
            t = waiters.poll();
        }
    }

    void push(NatsMessage msg) {
        this.queue.add(msg);
        this.length.incrementAndGet();
        this.sizeInBytes.getAndAdd(msg.getSizeInBytes());
        signalOne();
    }

    void waitForTimeout(Duration timeout) throws InterruptedException {
        long timeoutNanos = (timeout != null) ? timeout.toNanos() : -1;

        if (timeoutNanos >= 0) {
            Thread t = Thread.currentThread();
            long now = System.nanoTime();
            long start = now;

            // Then start waiting
            while (this.length.get() == 0 && this.running.get()) {
                if (timeoutNanos > 0) { // If it is 0, keep it as zero, otherwise reduce based on time
                    now = System.nanoTime();
                    timeoutNanos = timeoutNanos - (now - start);
                    start = now;

                    if (timeoutNanos <= 0) { // just in case we hit it exactly
                        break;
                    }
                }

                waiters.add(t);
                LockSupport.parkNanos(timeoutNanos);
                waiters.remove(t);

                if (Thread.interrupted()) {
                    throw new InterruptedException("Interrupted during timeout");
                }
            }
        }
    }

    NatsMessage pop(Duration timeout) throws InterruptedException {
        if (!this.running.get()) {
            return null;
        }

        NatsMessage retVal = this.queue.poll();

        if (retVal == null) {
            waitForTimeout(timeout);

            if (!this.running.get()) {
                return null;
            }

            retVal = this.queue.poll();
        }

        if(retVal != null) {
            this.sizeInBytes.getAndAdd(-retVal.getSizeInBytes());
            this.length.decrementAndGet();
            signalIfNotEmpty();
        }

        return retVal;
    }

    // Waits up to the timeout to try to accumulate multiple messages
    // Use the next field to read the entire set accumulated.
    // maxSize and maxMessages are both checked and if either is exceeded
    // the method returns.
    //
    // A timeout of 0 will wait indefinitely
    NatsMessage accumulate(long maxSize, long maxMessages, Duration timeout)
            throws InterruptedException {

        if (!this.singleThreadedReader) {
            throw new IllegalStateException("Accumulate is only supported in single reader mode.");
        }

        if (!this.running.get()) {
            return null;
        }

        NatsMessage msg = this.queue.poll();

        if (msg == null) {
            waitForTimeout(timeout);
            
            if (!this.running.get() || (this.queue.peek() == null)) {
                return null;
            }

            msg = this.queue.poll();
        }

        long size = msg.getSizeInBytes();

        if (maxMessages <= 1 || size >= maxSize) {
            this.sizeInBytes.addAndGet(-size);
            this.length.decrementAndGet();
            signalIfNotEmpty();
            return msg;
        }

        long count = 1;
        NatsMessage cursor = msg;

        while (cursor != null) {
            NatsMessage next = this.queue.peek();
            if (next != null) {
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

        signalIfNotEmpty();
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
        if (this.running.get()) {
            throw new IllegalStateException("Filter is only supported when the queue is paused");
        }
    
        ConcurrentLinkedQueue<NatsMessage> newQueue = new ConcurrentLinkedQueue<>();
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
    }
}