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
import java.util.ArrayList;
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

    protected List<MessageManager> managers;

    NatsJetStreamSubscription(String sid, String subject, String queueName,
                              NatsConnection connection, NatsDispatcher dispatcher,
                              NatsJetStream js,
                              String stream, String consumer,
                              MessageManager... managers) {
        super(sid, subject, queueName, connection, dispatcher);
        this.js = js;
        this.stream = stream;
        this.consumerName = consumer;

        this.managers = new ArrayList<>();
        for (MessageManager mm : managers) {
            if (mm != null) {
                this.managers.add(mm);
                mm.setSub(this);
            }
        }
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

    List<MessageManager> getManagers() { return managers; } // internal, for testing

    @Override
    void invalidate() {
        for (MessageManager mm : managers) {
            mm.shutdown();
        }
        super.invalidate();
    }

    @Override
    public Message nextMessage(Duration timeout) throws InterruptedException, IllegalStateException {
        if (timeout == null) {
            return nextMsgWaitForever();
        }
        return nextMessage(timeout.toMillis());
    }

    @Override
    public Message nextMessage(long timeoutMillis) throws InterruptedException, IllegalStateException {
        if (timeoutMillis == 0) {
            return nextMsgWaitForever();
        }

        if (timeoutMillis < 0) {
            return nextMsgNoWait();
        }

        return nextMessageWithEndTime(System.currentTimeMillis() + timeoutMillis);
    }

    boolean anyManaged(Message msg) {
        for (MessageManager mm : managers) {
            if (mm.manage(msg)) {
                return true;
            }
        }
        return false;
    }

    protected Message nextMsgWaitForever() throws InterruptedException {
        Message msg = nextMessageInternal(Duration.ZERO);
        while (msg == null || anyManaged(msg)) {
            msg = nextMessageInternal(Duration.ZERO);
        }
        return msg;
    }

    private static final Duration NO_WAIT_DURATION = Duration.ofMillis(-1);

    protected Message nextMsgNoWait() throws InterruptedException {
        Message msg = nextMessageInternal(NO_WAIT_DURATION);
        while (msg != null && anyManaged(msg)) {
            msg = nextMessageInternal(NO_WAIT_DURATION);
        }
        return msg;
    }

    protected Message nextMessageWithEndTime(long endTime) throws InterruptedException {
        // timeout > 0 process as many messages we can in that time period
        // If we get a message that either manager handles, we try again, but
        // with a shorter timeout based on what we already used up
        // starting millis of at least 1 ensure at least 1 try at the queue
        long millis = Math.max(1, endTime - System.currentTimeMillis());
        while (millis > 0) {
            Message msg = nextMessageInternal(Duration.ofMillis(millis));
            if (msg != null && !anyManaged(msg)) { // not null and not managed means JS Message
                return msg;
            }
            millis = endTime - System.currentTimeMillis();
        }
        return null;
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
