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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

class MessageQueue {
    private final static int STOPPED = 0;
    private final static int RUNNING = 1;
    private final static int DRAINING = 2;

    public static final int MAX_SPINS = 200; // Default max spins
    public static final int SPIN_WAIT = 50;
    public static final int MAX_SPIN_TIME = SPIN_WAIT * MAX_SPINS;

    private final AtomicLong length;
    private final AtomicLong sizeInBytes;
    private final AtomicInteger running;
    private final boolean singleThreadedReader;
    private final int maxSpins;
    private final ConcurrentLinkedQueue<NatsMessage> queue;
    private final ConcurrentLinkedQueue<Thread> waiters;
    private final Lock filterLock;

    MessageQueue(boolean singleReaderMode) {
        this.queue = new ConcurrentLinkedQueue<>();
        this.running = new AtomicInteger(RUNNING);
        this.sizeInBytes = new AtomicLong(0);
        this.length = new AtomicLong(0);

        this.filterLock = new ReentrantLock();
        
        this.waiters = new ConcurrentLinkedQueue<>();
        this.singleThreadedReader = singleReaderMode;

        String os = System.getProperty("os.name");
        os = os != null ? os.toLowerCase() : "";

        // Windows does not like spin locks, see issue #224
        if(os.contains("windows")) {
            this.maxSpins = 0;
        } else {
            this.maxSpins = MAX_SPINS;
        }
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
        signalAll();
    }

    void resume() {
        this.running.set(RUNNING);
        signalAll();
    }

    void drain() {
        this.running.set(DRAINING);
        signalAll();
    }

    boolean isDrained() {
        return this.running.get() == DRAINING && this.length() == 0;
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

        // If we aren't running, then we need to obey the filter lock
        // to avoid ordering problems
        if(!this.isRunning()) {
            this.filterLock.lock();
            this.queue.add(msg);
            this.filterLock.unlock();
            this.sizeInBytes.getAndAdd(msg.getSizeInBytes());
            this.length.incrementAndGet();
            signalOne();
            return;
        }

        this.queue.add(msg);
        this.sizeInBytes.getAndAdd(msg.getSizeInBytes());
        this.length.incrementAndGet();
        signalOne();
    }

    NatsMessage waitForTimeout(Duration timeout) throws InterruptedException {
        long timeoutNanos = (timeout != null) ? timeout.toNanos() : -1;
        NatsMessage retVal = null;

        if (timeoutNanos >= 0) {
            Thread t = Thread.currentThread();
            long start = System.nanoTime();

            // Semi-spin for at most MAX_SPIN_TIME
            if (timeoutNanos > MAX_SPIN_TIME) {
                int count = 0;
                while (this.isRunning() && (retVal = this.queue.poll()) == null && count < this.maxSpins) {

                    if (this.isDraining()) {
                        break;
                    }

                    count++;
                    LockSupport.parkNanos(SPIN_WAIT);
                }
            }

            if (retVal != null) {
                return retVal;
            }
            
            long now = start;

            while (this.isRunning() && (retVal = this.queue.poll()) == null) {
                
                if (this.isDraining()) {
                    break;
                }
                
                if (timeoutNanos > 0) { // If it is 0, keep it as zero, otherwise reduce based on time
                    now = System.nanoTime();
                    timeoutNanos = timeoutNanos - (now - start); //include the semi-spin time
                    start = now;

                    if (timeoutNanos <= 0) { // just in case we hit it exactly
                        break;
                    }
                }

                // Thread.sleep(1000) <- use this to test the "isEmpty" fix below
                // see https://github.com/nats-io/java-nats/issues/220 for discussion

                // once we are in the waiters, we can be signaled, but
                // until then we can't, so there is a possible timing bug
                // where we aren't in waiters, aren't checking queue and
                // miss the signalone when we are the only reader. By double
                // checking isEmpty we insure that we either get the signal in park
                // or we have a very short park here
                waiters.add(t);
                if (!this.queue.isEmpty()) {
                    LockSupport.parkNanos(SPIN_WAIT);
                } else if (timeoutNanos == 0) {
                    LockSupport.park();
                } else {
                    LockSupport.parkNanos(timeoutNanos);
                }
                waiters.remove(t);

                if (Thread.interrupted()) {
                    throw new InterruptedException("Interrupted during timeout");
                }
            }
        }

        return retVal;
    }

    NatsMessage pop(Duration timeout) throws InterruptedException {
        if (!this.isRunning()) {
            return null;
        }

        NatsMessage retVal = this.queue.poll();

        if (retVal == null && timeout != null) {
            retVal = waitForTimeout(timeout);
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
    //
    // Only works in single reader mode, because we want to maintain order.
    // accumulate reads off the concurrent queue one at a time, so if multiple
    // readers are present, you could get out of order message delivery.
    NatsMessage accumulate(long maxSize, long maxMessages, Duration timeout)
            throws InterruptedException {

        if (!this.singleThreadedReader) {
            throw new IllegalStateException("Accumulate is only supported in single reader mode.");
        }

        if (!this.isRunning()) {
            return null;
        }

        NatsMessage msg = this.queue.poll();

        if (msg == null) {
            msg = waitForTimeout(timeout);
            
            if (!this.isRunning() || (msg == null)) {
                return null;
            }
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
        if (this.isRunning()) {
            throw new IllegalStateException("Filter is only supported when the queue is paused");
        }
    
        this.filterLock.lock();
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
        this.filterLock.unlock();
    }
}