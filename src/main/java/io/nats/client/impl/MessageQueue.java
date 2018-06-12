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

class MessageQueue {
    private long length;
    private NatsMessage head;
    private NatsMessage tail;
    private ReentrantLock lock;
    private Condition condition;
    private boolean interrupted;

    MessageQueue() {
        this.lock = new ReentrantLock(false); // Look at fairness vs some other way to avoid fast writers
        this.condition = lock.newCondition();
        this.interrupted = false;
    }

    void interrupt() {
        this.lock.lock();
        try
        {
            this.interrupted = true;
            this.condition.signalAll();
        } finally {
            this.lock.unlock();
        }
    }

    void reset() {
        this.lock.lock();
        try
        {
            this.interrupted = false;
            this.condition.signalAll();
        } finally {
            this.lock.unlock();
        }
    }

    void push(NatsMessage msg) {
        lock.lock();
        try {
            if(length == 0){
                this.head = this.tail = msg;        
            }else{
                this.head.prev = msg;
                msg.next = this.head;
                this.head = msg;
            }
            this.length++;
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
        long timeoutNanos = (timeout != null) ? timeout.toNanos(): -1;

        lock.lock();
        try {
            if (timeoutNanos >= 0) {
                while ((this.length == 0) && !this.interrupted) {
                    if (timeoutNanos > 0) { // If it is 0, keep it, otherwise reduce for time elapsed
                        now = System.nanoTime();
                        timeoutNanos = timeoutNanos - (now-start);
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

            if(this.head == this.tail){
                this.head = this.tail = null;
            }else{
                this.tail = this.tail.prev;
                this.tail.next = null;
            }

            retVal.prev = null;

            this.length--;
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
    // accumulateTimeout sets how long to wait for the max condition AFTER the first message appears
    // TODO(sasbury): This is thread safe, but maybe it would be faster if we didn't worry about that.
    // The accumulate timeout may be unneeded, since the thread contention will act as an accumulator of sorts
    // 
    // A timeout of 0 will wait indefinitely
    NatsMessage accumulate(long maxSize, long maxMessages, Duration accumulateTimeout, Duration timeout) throws InterruptedException {
        NatsMessage oldestMessage = null;
        NatsMessage newestMessage = null; //first, but these should be read in reverse order, with this one last
        long now = System.nanoTime();
        long start = now;
        long timeoutNanos = (timeout != null) ? timeout.toNanos(): -1;
        long accumulateNanos = (accumulateTimeout != null) ? accumulateTimeout.toNanos(): -1;

        lock.lock();
        try {

            // First wait for any messages - using the timeout
            if (timeoutNanos >= 0) {
                while ((this.length == 0) && !this.interrupted) {
                    if (timeoutNanos > 0) { // If it is 0, keep it as zero, otherwise reduce based on time
                        now = System.nanoTime();
                        timeoutNanos = timeoutNanos - (now-start);
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
            
            // Wait up to the accumulate timeout, or we get the max condition
            start = System.nanoTime();

            if (accumulateNanos >= 0) {
                while ((findMax(this.tail, maxSize, maxMessages) == null) && !this.interrupted) {
                    if(accumulateNanos > 0 ){ // Same as timeout, reduce if > 0 for time served
                        now = System.nanoTime();
                        accumulateNanos = accumulateNanos - (now-start);
                        start = now;

                        if (accumulateNanos <= 0) { // equals in case we happen to hit it exactly
                            break;
                        }
                    }

                    condition.await(accumulateNanos, TimeUnit.NANOSECONDS);
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
            if(this.head == this.tail || this.head == newestMessage){
                this.head = this.tail = null;
            } else{
                this.tail = newestMessage.prev;
                this.tail.next = null;
            }

            newestMessage.prev = null;
            oldestMessage.next = null;

            // Update the length
            NatsMessage cursor = oldestMessage;
            while (cursor != null) {
                this.length--;
                cursor = cursor.prev;
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
        long size = start.getSize();
        long count = 1;

        while (cursor != null) {
            if (cursor.prev != null) {
                long s = cursor.prev.getSize();
                
                if ((size + s) < maxSize) { // keep going
                    cursor = cursor.prev;
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
}