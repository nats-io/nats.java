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

package io.nats.client;

import static io.nats.client.Nats.ERR_BAD_SUBSCRIPTION;
import static io.nats.client.Nats.ERR_CONNECTION_CLOSED;
import static io.nats.client.Nats.ERR_MAX_MESSAGES;
import static io.nats.client.Nats.ERR_SLOW_CONSUMER;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

class SyncSubscriptionImpl extends SubscriptionImpl implements SyncSubscription {
    private final Map<Long, Thread> threads = new ConcurrentHashMap<Long, Thread>();

    SyncSubscriptionImpl(ConnectionImpl nc, String subj, String queue) {
        super(nc, subj, queue);
    }

    @Override
    public void close() {
        lock();
        try {
            for (Map.Entry<Long, Thread> entry : threads.entrySet()) {
                if (entry.getKey() != Thread.currentThread().getId()) {
                    entry.getValue().interrupt();
                }
            }
        } finally {
            unlock();
        }
        super.close();
    }

    @Override
    public Message nextMessage() throws IOException, InterruptedException {
        return nextMessage(-1);
    }

    @Override
    public Message nextMessage(long timeout)
            throws IOException, InterruptedException {
        return nextMessage(timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public Message nextMessage(long timeout, TimeUnit unit)
            throws IOException, InterruptedException {

        lock();
        if (connClosed) {
            unlock();
            throw new IllegalStateException(ERR_CONNECTION_CLOSED);
        }
        if (mch == null) {
            if ((this.max > 0) && (delivered >= this.max)) {
                unlock();
                throw new IOException(ERR_MAX_MESSAGES);
            } else if (closed) {
                unlock();
                throw new IllegalStateException(ERR_BAD_SUBSCRIPTION);
            }
        }
        if (sc) {
            sc = false;
            unlock();
            throw new IOException(ERR_SLOW_CONSUMER);
        }

        // snapshot
        final ConnectionImpl nc = (ConnectionImpl) this.getConnection();
        final BlockingQueue<Message> localChannel = mch;
        final long localMax = max;
        // boolean chanClosed = localChannel.isClosed();
        unlock();
        Message msg = null;
        // Wait until a message is available
        threads.put(Thread.currentThread().getId(), Thread.currentThread());
        try {
            if (localChannel == null) {
                throw new IllegalStateException(ERR_CONNECTION_CLOSED);
            }
            if (timeout >= 0) {
                msg = localChannel.poll(timeout, unit);
            } else {
                msg = localChannel.take();
            }
        } finally {
            threads.remove(Thread.currentThread().getId());
        }

        if (msg != null) {
            // Update some stats
            lock();
            long delivered;
            try {
                this.delivered++;
                delivered = this.delivered;
                pMsgs--;
                pBytes -= (msg.getData() != null ? msg.getData().length : 0);
            } finally {
                unlock();
            }

            if (localMax > 0) {
                if (delivered > localMax) {
                    throw new IOException(ERR_MAX_MESSAGES);
                }
                // Remove subscription if we have reached max.
                if (delivered == localMax) {
                    nc.mu.lock();
                    try {
                        nc.removeSub(this);
                    } finally {
                        nc.mu.unlock();
                    }
                }
            }
        }
        return msg;
    }
}
