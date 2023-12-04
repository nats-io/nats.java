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
import io.nats.client.MessageHandler;
import io.nats.client.Subscription;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

class NatsSubscription extends NatsConsumer implements Subscription {

    private String subject;
    private final String queueName;
    private String sid;

    private NatsDispatcher dispatcher;
    private MessageQueue incoming;

    private final AtomicLong unSubMessageLimit;

    private Function<NatsMessage, Boolean> beforeQueueProcessor;

    NatsSubscription(String sid, String subject, String queueName, NatsConnection connection, NatsDispatcher dispatcher) {
        super(connection);
        this.subject = subject;
        this.queueName = queueName;
        this.sid = sid;
        this.dispatcher = dispatcher;
        this.unSubMessageLimit = new AtomicLong(-1);

        if (this.dispatcher == null) {
            this.incoming = new MessageQueue(false);
        }

        setBeforeQueueProcessor(null);
    }

    void reSubscribe(String newDeliverSubject) {
        connection.sendUnsub(this, 0);
        if (dispatcher == null) {
            connection.remove(this);
            sid = connection.reSubscribe(this, newDeliverSubject, queueName);
        }
        else {
            MessageHandler handler = dispatcher.getSubscriptionHandlers().get(sid);
            dispatcher.remove(this);
            sid = dispatcher.reSubscribe(this, newDeliverSubject, queueName, handler);
        }
        subject = newDeliverSubject;
    }

    public boolean isActive() {
        return (this.dispatcher != null || this.incoming != null);
    }

    void setBeforeQueueProcessor(Function<NatsMessage, Boolean> beforeQueueProcessor) {
        this.beforeQueueProcessor = beforeQueueProcessor == null ? m -> true : beforeQueueProcessor;
    }

    public Function<NatsMessage, Boolean> getBeforeQueueProcessor() {
        return beforeQueueProcessor;
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

    @Override
    public Message nextMessage(long timeoutMillis) throws InterruptedException, IllegalStateException {
        return nextMessageInternal(Duration.ofMillis(timeoutMillis));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Message nextMessage(Duration timeout) throws InterruptedException, IllegalStateException {
        return nextMessageInternal(timeout);
    }

    protected NatsMessage nextMessageInternal(Duration timeout) throws InterruptedException {
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

        if (msg != null) {
            this.incrementDeliveredCount();
        }

        if (this.reachedUnsubLimit()) {
            this.connection.invalidate(this);
        }

        if(getDeserializer() != null) {
            try{
                byte[] data = getDeserializer().decode(subject, msg.data);
                msg.data = data;
            }catch(IOException e){
                throw new IllegalStateException("Unable to decode data", e);
            }
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