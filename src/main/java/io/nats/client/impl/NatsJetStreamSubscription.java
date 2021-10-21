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

    protected final String stream;
    protected final String consumerName;
    protected final String deliver;

    protected final AutoStatusManager asm;

    static NatsJetStreamSubscription getInstance(String sid, String subject, String queueName,
                                                 NatsConnection connection, NatsDispatcher dispatcher,
                                                 AutoStatusManager asm,
                                                 NatsJetStream js, boolean pullMode,
                                                 String stream, String consumer, String deliver) {
        // pull gets a full implementation
        if (pullMode) {
            return new NatsJetStreamPullSubscription(sid, subject, queueName, connection, dispatcher, asm, js, stream, consumer);
        }

        return new NatsJetStreamSubscription(sid, subject, queueName, connection, dispatcher, asm, js, stream, consumer, deliver);
    }

    protected NatsJetStreamSubscription(String sid, String subject, String queueName,
                                      NatsConnection connection, NatsDispatcher dispatcher,
                                      AutoStatusManager asm,
                                      NatsJetStream js,
                                      String stream, String consumer, String deliver) {
        super(sid, subject, queueName, connection, dispatcher);
        this.asm = asm;
        this.js = js;
        this.stream = stream;
        this.consumerName = consumer;
        this.deliver = deliver;
    }

    String getConsumerName() {
        return consumerName;
    }

    String getStream() {
        return stream;
    }

    String getDeliverSubject() {
        return deliver;
    }

    boolean isPullMode() {
        return false;
    }

    AutoStatusManager getAsm() { return asm; } // internal, for testing

    @Override
    void invalidate() {
        asm.shutdown();
        super.invalidate();
    }

    @Override
    public Message nextMessage(Duration timeout) throws InterruptedException, IllegalStateException {
        if (timeout == null || timeout.toMillis() <= 0) {
            return nextMsgNullOrLteZero(timeout);
        }

        return nextMessageWithEndTime(System.currentTimeMillis() + timeout.toMillis());
    }

    @Override
    public Message nextMessage(long timeoutMillis) throws InterruptedException, IllegalStateException {
        if (timeoutMillis <= 0) {
            return nextMsgNullOrLteZero(Duration.ZERO);
        }

        return nextMessageWithEndTime(System.currentTimeMillis() + timeoutMillis);
    }

    protected Message nextMsgNullOrLteZero(Duration timeout) throws InterruptedException {
        // timeout null means don't wait at all, timeout <= 0 means wait forever
        // until we get an actual no (null) message or we get a message
        // that the manager (asm) does not handle (asm.preProcess would be false)
        Message msg = nextMessageInternal(timeout);
        while (msg != null && asm.manage(msg)) {
            msg = nextMessageInternal(timeout);
        }
        return msg;
    }

    protected Message nextMessageWithEndTime(long endTime) throws InterruptedException {
        // timeout >= 0 process as many messages we can in that time period
        // if we get a message that the asm handles, we try again, but
        // with a shorter timeout based on what we already used up
        long millis = endTime - System.currentTimeMillis();
        while (millis > 0) {
            Message msg = nextMessageInternal(Duration.ofMillis(millis));
            if (msg != null && !asm.manage(msg)) { // not null and not managed means JS Message
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
                ", deliver='" + deliver + '\'' +
                ", isPullMode=" + isPullMode() +
                '}';
    }
}
