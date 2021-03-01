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

import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Subscription;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

class NatsSubscription extends NatsConsumer implements Subscription {

    private String subject;
    private String queueName;
    private String sid;

    private NatsDispatcher dispatcher;
    private MessageQueue incoming;

    private AtomicLong unSubMessageLimit;

    NatsSubscription(String sid, String subject, String queueName, NatsConnection connection,
            NatsDispatcher dispatcher) {
        super(connection);
        this.subject = subject;
        this.queueName = queueName;
        this.sid = sid;
        this.dispatcher = dispatcher;
        this.unSubMessageLimit = new AtomicLong(-1);

        if (this.dispatcher == null) {
            this.incoming = new MessageQueue(false);
        }
    }

    public boolean isActive() {
        return (this.dispatcher != null || this.incoming != null);
    }

    void invalidate() {
        if (this.incoming != null) {
            this.incoming.pause();
        }
        this.dispatcher = null;
        this.incoming = null;
    }

    void setUnsubLimit(long cd) {
        this.unSubMessageLimit.set(cd);
    }

    boolean reachedUnsubLimit() {
        long max = this.unSubMessageLimit.get();
        long recv = this.getDeliveredCount();
        return (max > 0) && (max <= recv);
    }

    String getSID() {
        return this.sid;
    }

    NatsDispatcher getNatsDispatcher() {
        return this.dispatcher;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    MessageQueue getMessageQueue() {
        return this.incoming;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Dispatcher getDispatcher() {
        return this.dispatcher;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSubject() {
        return this.subject;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getQueueName() {
        return this.queueName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Message nextMessage(Duration timeout) throws InterruptedException, IllegalStateException {
        if (this.dispatcher != null) {
            throw new IllegalStateException(
                    "Subscriptions that belong to a dispatcher cannot respond to nextMessage directly.");
        } else if (this.incoming == null) {
            throw new IllegalStateException("This subscription is inactive.");
        }

        NatsMessage msg = incoming.pop(timeout);

        if (this.incoming == null || !this.incoming.isRunning()) { // We were unsubscribed while waiting
            throw new IllegalStateException("This subscription became inactive.");
        }

        this.incrementDeliveredCount();

        if (this.reachedUnsubLimit()) {
            this.connection.invalidate(this);
        }

        return msg;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unsubscribe() {
        if (this.dispatcher != null) {
            throw new IllegalStateException(
                    "Subscriptions that belong to a dispatcher cannot respond to unsubscribe directly.");
        } else if (this.incoming == null) {
            throw new IllegalStateException("This subscription is inactive.");
        }

        if (isDraining()) { // No op while draining
            return;
        }

        this.connection.unsubscribe(this, -1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Subscription unsubscribe(int after) {
        if (this.dispatcher != null) {
            throw new IllegalStateException(
                    "Subscriptions that belong to a dispatcher cannot respond to unsubscribe directly.");
        } else if (this.incoming == null) {
            throw new IllegalStateException("This subscription is inactive.");
        }

        if (isDraining()) { // No op while draining
            return this;
        }

        this.connection.unsubscribe(this, after);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    void sendUnsubForDrain() {
        this.connection.sendUnsub(this, -1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    void cleanUpAfterDrain() {
        this.connection.invalidate(this);
    }
}