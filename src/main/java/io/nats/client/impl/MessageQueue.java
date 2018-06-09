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

    private long size;
    private NatsMessage head;
    private NatsMessage tail;
    private ReentrantLock lock;
    private Condition condition;
    private boolean interrupted;

    MessageQueue() {
        this.lock = new ReentrantLock();
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
            if(size == 0){
                this.head = this.tail = msg;        
            }else{
                this.head.prev = msg;
                this.head = msg;
            }
            this.size++;
        } finally {
            condition.signalAll();
            lock.unlock();
        }
    }

    // Returns a message or waits up to the timeout, returns null if timed out
    // A timeout of 0 will wait indefinitely
    NatsMessage pop(Duration timeout) throws InterruptedException {
        NatsMessage retVal = null;
        long start = 0;
        long timeoutMillis = (timeout != null) ? timeout.toMillis(): -1;

        lock.lock();
        try {
            while (this.size == 0 && !this.interrupted) {
                if (timeoutMillis < 0) {
                    return null;
                } else if (start == 0) {
                    start = System.currentTimeMillis();
                } else if (timeoutMillis > 0) { // If it is 0, keep going forever
                    // Keep the timeout up to date
                    long now = System.currentTimeMillis();
                    timeoutMillis = (timeoutMillis - (now-start));
                    start = now;

                    if (timeoutMillis <= 0) { // just in case we hit it exactly
                        return retVal;
                    }
                }

                condition.await(timeoutMillis, TimeUnit.MILLISECONDS);
            }
            
            if (this.interrupted) {
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

            this.size--;
        } finally {
            condition.signalAll();
            lock.unlock();
        }

        return retVal;
    }

    // Returns a message or null
    NatsMessage popNow() throws InterruptedException {
        return pop(null);
    }
}