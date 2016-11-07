/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import static io.nats.client.Nats.ERR_BAD_SUBSCRIPTION;
import static io.nats.client.Nats.ERR_CONNECTION_CLOSED;
import static io.nats.client.Nats.ERR_MAX_MESSAGES;
import static io.nats.client.Nats.ERR_SLOW_CONSUMER;
import static io.nats.client.Nats.ERR_TIMEOUT;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class SyncSubscriptionImpl extends SubscriptionImpl implements SyncSubscription {
    private Map<Long, Thread> threads = new ConcurrentHashMap<Long, Thread>();

    protected SyncSubscriptionImpl(ConnectionImpl nc, String subj, String queue) {
        super(nc, subj, queue);
    }

    @Override
    public void close() {
        mu.lock();
        try {
            for (long key : threads.keySet()) {
                if (key != Thread.currentThread().getId()) {
                    threads.get(key).interrupt();
                }
            }
        } finally {
            mu.unlock();
        }
        super.close();
    }

    @Override
    public Message nextMessage() throws IOException, InterruptedException {
        Message msg = null;
        try {
            msg = nextMessage(-1);
        } catch (TimeoutException e) {
            // Can't happen
        }
        return msg;
    }

    @Override
    public Message nextMessage(long timeout)
            throws IOException, TimeoutException, InterruptedException {
        return nextMessage(timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public Message nextMessage(long timeout, TimeUnit unit)
            throws IOException, TimeoutException, InterruptedException {

        // TODO this call should just return null vs. throwing TimeoutException. TimeoutException
        // was here due to historical implementation tradeoffs that no longer apply.
        mu.lock();
        if (connClosed) {
            mu.unlock();
            throw new IllegalStateException(ERR_CONNECTION_CLOSED);
        }
        if (mch == null) {
            if ((this.max > 0) && (delivered >= this.max)) {
                mu.unlock();
                throw new IOException(ERR_MAX_MESSAGES);
            } else if (closed) {
                mu.unlock();
                throw new IllegalStateException(ERR_BAD_SUBSCRIPTION);
            }
        }
        if (sc) {
            sc = false;
            mu.unlock();
            throw new IOException(ERR_SLOW_CONSUMER);
        }

        // snapshot
        final ConnectionImpl localConn = (ConnectionImpl) this.getConnection();
        // final BlockingQueue<Message> localChannel = mch;
        final long localMax = max;
        // boolean chanClosed = localChannel.isClosed();
        mu.unlock();
        Message msg = null;
        // Wait until a message is available
        threads.put(Thread.currentThread().getId(), Thread.currentThread());
        try {
            if (timeout >= 0) {
                msg = mch.poll(timeout, unit);
                if (msg == null) {
                    throw new TimeoutException(ERR_TIMEOUT);
                }
            } else {
                msg = mch.take();
            }
        } finally {
            threads.remove(Thread.currentThread().getId());
        }

        if (msg != null) {
            // Update some stats
            mu.lock();
            this.delivered++;
            long delivered = this.delivered;
            pMsgs--;
            pBytes -= (msg.getData() != null ? msg.getData().length : 0);
            mu.unlock();

            if (localMax > 0) {
                if (delivered > localMax) {
                    throw new IOException(ERR_MAX_MESSAGES);
                }
                // Remove subscription if we have reached max.
                if (delivered == localMax) {
                    localConn.mu.lock();
                    try {
                        localConn.removeSub(this);
                    } finally {
                        localConn.mu.unlock();
                    }
                }
            }
        }
        return msg;
    }
}
