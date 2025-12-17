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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.nats.client.impl.MarkerMessage.POISON_PILL;

class ConsumerMessageQueue {
    private static final int PAUSED = 0;
    private static final int RUNNING = 1;
    private static final int DRAINING = 2;

    private final LinkedBlockingQueue<NatsMessage> queue;
    private final AtomicLong length;
    private final AtomicLong sizeInBytes;
    private final AtomicInteger running;

    ConsumerMessageQueue() {
        running = new AtomicInteger(RUNNING);
        queue = new LinkedBlockingQueue<>();
        length = new AtomicLong(0);
        sizeInBytes = new AtomicLong(0);
    }

    boolean isRunning() {
        return running.get() != PAUSED;
    }

    boolean isDraining() {
        return running.get() == DRAINING;
    }

    void pause() {
        running.set(PAUSED);
        queue.offer(POISON_PILL);
    }

    void drain() {
        running.set(DRAINING);
        queue.offer(POISON_PILL);
    }

    void resume() {
        running.set(RUNNING);
    }

    boolean isDrained() {
        return running.get() == DRAINING && length.get() == 0;
    }

    void push(NatsMessage msg) {
        if (queue.offer(msg)) {
            length.incrementAndGet();
            sizeInBytes.addAndGet(msg.getSizeInBytes());
        }
    }

    NatsMessage pop(Duration timeout) throws InterruptedException {
        if (!isRunning()) {
            return null;
        }

        NatsMessage msg = null;
        if (timeout == null || isDraining()) { // try immediately
            msg = queue.poll();
        }
        else {
            long nanos = timeout.toNanos();
            if (nanos > 0) {
                msg = queue.poll(nanos, TimeUnit.NANOSECONDS);
            }
            else {
                // A value < 1 means wait forever. Stop and drain add a poison pill
                // so if you started waiting forever and then the queue is stopped
                // or drained, this will return.
                msg = queue.poll(3650, TimeUnit.DAYS);
            }
        }

        if (msg == null || msg == POISON_PILL) {
            return null;
        }
        length.decrementAndGet();
        sizeInBytes.addAndGet(-msg.getSizeInBytes());
        return msg;
    }

    long length() {
        return length.get();
    }

    long sizeInBytes() {
        return sizeInBytes.get();
    }
}
