/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import static io.nats.client.Constants.ERR_BAD_SUBSCRIPTION;
import static io.nats.client.Constants.ERR_CONNECTION_CLOSED;
import static io.nats.client.Constants.ERR_MAX_MESSAGES;
import static io.nats.client.Constants.ERR_SLOW_CONSUMER;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class SyncSubscriptionImpl extends SubscriptionImpl implements SyncSubscription {

    protected SyncSubscriptionImpl(ConnectionImpl nc, String subj, String queue) {
        super(nc, subj, queue);
    }

    @Override
    public Message nextMessage() throws IOException {
        Message msg = null;
        try {
            msg = nextMessage(-1);
        } catch (TimeoutException e) {
            // Can't happen
        }
        return msg;
    }

    @Override
    public Message nextMessage(long timeout) throws IOException, TimeoutException {
        return nextMessage(timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public Message nextMessage(long timeout, TimeUnit unit) throws IOException, TimeoutException {
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
        if (sc == true) {
            sc = false;
            mu.unlock();
            throw new IOException(ERR_SLOW_CONSUMER);
        }

        // snapshot
        final ConnectionImpl localConn = (ConnectionImpl) this.getConnection();
        final BlockingQueue<Message> localChannel = mch;
        final long localMax = max;
        // boolean chanClosed = localChannel.isClosed();
        mu.unlock();
        Message msg = null;

        try {
            if (timeout >= 0) {
                msg = localChannel.poll(timeout, unit);
                if (msg == null) {
                    throw new TimeoutException("Timed out waiting for message");
                }
            } else {
                msg = localChannel.take();
            }
        } catch (InterruptedException e) {
            logger.warn("Interrupted");
            Thread.currentThread().interrupt();
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
