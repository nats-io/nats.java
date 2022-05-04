// Copyright 2020 The NATS Authors
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

import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.support.NatsJetStreamConstants;

import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;

/**
 * This is a JetStream specific subscription.
 */
public class NatsJetStreamSubscription extends NatsSubscription implements JetStreamSubscription, NatsJetStreamConstants {

    public static final String SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL = "Subscription type does not support pull.";

    protected final NatsJetStream js;

    protected String stream;
    protected String consumerName;

    protected MessageManager[] managers;

    NatsJetStreamSubscription(String sid, String subject, String queueName,
                              NatsConnection connection, NatsDispatcher dispatcher,
                              NatsJetStream js,
                              String stream, String consumer,
                              MessageManager[] inManagers) {
        super(sid, subject, queueName, connection, dispatcher);
        this.js = js;
        this.stream = stream;
        this.consumerName = consumer; // might be null, someone will call setConsumerName

        managers = inManagers;
        for (MessageManager mm : managers) {
            mm.setSub(this);
        }
    }

    void setConsumerName(String consumerName) {
        this.consumerName = consumerName;
    }

    String getConsumerName() {
        return consumerName;
    }

    String getStream() {
        return stream;
    }

    boolean isPullMode() {
        return false;
    }

    MessageManager[] getManagers() { return managers; } // internal, for testing

    @Override
    void invalidate() {
        for (MessageManager mm : managers) {
            mm.shutdown();
        }
        super.invalidate();
    }

    @Override
    public Message nextMessage(Duration timeout) throws InterruptedException, IllegalStateException {
        if (timeout == null || timeout.toMillis() <= 0) {
            return _nextUnmanagedNullOrLteZero(timeout);
        }

        return _nextUnmanaged(timeout.toMillis());
    }

    @Override
    public Message nextMessage(long timeoutMillis) throws InterruptedException, IllegalStateException {
        if (timeoutMillis <= 0) {
            return _nextUnmanagedNullOrLteZero(Duration.ZERO);
        }

        return _nextUnmanaged(timeoutMillis);
    }

    protected Message _nextUnmanagedNullOrLteZero(Duration timeout) throws InterruptedException {
        // timeout null means don't wait at all, timeout <= 0 means wait forever
        // until we get an actual no (null) message or we get a message
        // that the managers do not handle
        Message msg = nextMessageInternal(timeout);
        while (msg != null && anyManaged(msg)) {
            msg = nextMessageInternal(timeout);
        }
        return msg;
    }

    protected static final long MIN_MILLIS = 20;
    protected static final long EXPIRE_LESS_MILLIS = 10;

    protected Message _nextUnmanaged(long timeout) throws InterruptedException {

        // timeout > 0 process as many messages we can in that time period
        // If we get a message that either manager handles, we try again, but
        // with a shorter timeout based on what we already used up
        long elapsed = 0;
        long start = System.currentTimeMillis();
        while (elapsed < timeout) {
            Message msg = nextMessageInternal( Duration.ofMillis(Math.max(MIN_MILLIS, timeout - elapsed)) );
            if (msg == null) {
                return null; // normal timeout
            }
            if (!anyManaged(msg)) { // not managed means JS Message
                return msg;
            }
            // managed so try again while we have time
            elapsed = System.currentTimeMillis() - start;
        }
        return null;
    }

    boolean anyManaged(Message msg) {
        for (MessageManager mm : managers) {
            if (mm.manage(msg)) {
                return true;
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pull(int batchSize) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullNoWait(int batchSize) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullNoWait(int batchSize, Duration expiresIn) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullNoWait(int batchSize, long expiresInMillis) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullExpiresIn(int batchSize, Duration expiresIn) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullExpiresIn(int batchSize, long expiresInMillis) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Message> fetch(int batchSize, long maxWaitMillis) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Message> fetch(int batchSize, Duration maxWait) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Message> iterate(int batchSize, Duration maxWait) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Message> iterate(final int batchSize, long maxWaitMillis) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerInfo getConsumerInfo() throws IOException, JetStreamApiException {
        return js.lookupConsumerInfo(stream, consumerName);
    }

    @Override
    public String toString() {
        return "NatsJetStreamSubscription{" +
                "consumer='" + consumerName + '\'' +
                ", stream='" + stream + '\'' +
                ", deliver='" + getSubject() + '\'' +
                ", isPullMode=" + isPullMode() +
                '}';
    }
}
