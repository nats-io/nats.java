// Copyright 2021 The NATS Authors
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

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.DeliverPolicy;

import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * This is a JetStream specific subscription.
 */
public class NatsJetStreamOrderedSubscription implements JetStreamSubscription {

    private final NatsJetStream js;
    private final String subject;
    private final NatsDispatcher dispatcher;
    private final MessageHandler userHandler;
    private final boolean isAutoAck;
    private final SubscribeOptions so;
    private final String stream;
    private final ConsumerConfiguration serverCC;

    private JetStreamSubscription active;
    private long lastStreamSeq;
    private long expectedConsumerSeq;

    NatsJetStreamOrderedSubscription(NatsJetStream js, String subject, NatsDispatcher dispatcher, MessageHandler userHandler,
                                     boolean isAutoAck, SubscribeOptions so, String stream, ConsumerConfiguration serverCC) {
        this.js = js;
        this.subject = subject;
        this.dispatcher = dispatcher;
        this.userHandler = userHandler;
        this.isAutoAck = isAutoAck;
        this.so = so;
        this.stream = stream;
        this.serverCC = serverCC;
        lastStreamSeq = -1;
        expectedConsumerSeq = 1; // always starts at 1
    }

    void setActive(JetStreamSubscription sub) {
        this.active = sub;
        lastStreamSeq = -1;
        expectedConsumerSeq = 1; // always starts at 1
    }

    JetStreamSubscription getActive() {
        return active;
    }

    @Override
    public String toString() {
        if (active == null) {
            return "NatsJetStreamOrderedSubscription{" +
                "this=" + hashCode() +
                ", delegate=inactive" +
                ", stream='" + stream + '\'' +
                '}';
        }
        return "NatsJetStreamOrderedSubscription{" +
            "this=" + hashCode() +
            ", delegate=" + active.hashCode() +
            ", consumer='" + ((NatsJetStreamSubscription)active).getConsumerName() + '\'' +
            ", stream='" + stream + '\'' +
            ", deliver='" + ((NatsJetStreamSubscription)active).getDeliverSubject() + '\'' +
            '}';
    }

    MessageManager manager() {
        // new instance each time so the inOrder flag will be properly set
        return new MessageManager() {
            boolean outOfOrder = false;

            @Override
            public boolean manage(Message msg) {
                // if out of order return true which means already managed
                // never pass an out of order message to the user
                if (!outOfOrder) {
                    outOfOrder = checkForOutOfOrder(msg) == null;
                }
                return outOfOrder;
            }
        };
    }

    private Message checkForOutOfOrder(Message msg) {
        if (msg != null) {
            long receivedConsumerSeq = msg.metaData().consumerSequence();
            if (expectedConsumerSeq != receivedConsumerSeq) {
                try {
                    if (dispatcher == null) {
                        active.unsubscribe();
                    }
                    else {
                        dispatcher.unsubscribe(active);
                    }
                } catch (RuntimeException re) {
                    js.conn.processException(re);
                } finally {
                    active = null;
                }

                ConsumerConfiguration userCC = ConsumerConfiguration.builder(serverCC)
                    .deliverPolicy(DeliverPolicy.ByStartSequence)
                    .startSequence(lastStreamSeq + 1)
                    .build();
                try {
                    active = js.finishCreateSubscription(subject, dispatcher, userHandler,
                        isAutoAck, false, so, stream, null,
                        userCC, null, null, null, this);
                } catch (Exception e) {
                    e.printStackTrace();
                    active = null;
                    js.conn.processException(e);
                    if (dispatcher == null) { // synchronous
                        throw new IllegalStateException("Ordered subscription fatal error.", e);
                    }
                }
                return null;
            }
            lastStreamSeq = msg.metaData().streamSequence();
            expectedConsumerSeq = receivedConsumerSeq + 1;
        }
        return msg;
    }

    @Override
    public void pull(int batchSize) {
        active.pull(batchSize);
    }

    @Override
    public void pullNoWait(int batchSize) {
        active.pullNoWait(batchSize);
    }

    @Override
    public void pullExpiresIn(int batchSize, Duration expiresIn) {
        active.pullExpiresIn(batchSize, expiresIn);
    }

    @Override
    public void pullExpiresIn(int batchSize, long expiresInMillis) {
        active.pullExpiresIn(batchSize, expiresInMillis);
    }

    @Override
    public List<Message> fetch(int batchSize, long maxWaitMillis) {
        return active.fetch(batchSize, maxWaitMillis);
    }

    @Override
    public List<Message> fetch(int batchSize, Duration maxWait) {
        return active.fetch(batchSize, maxWait);
    }

    @Override
    public Iterator<Message> iterate(int batchSize, Duration maxWait) {
        return active.iterate(batchSize, maxWait);
    }

    @Override
    public Iterator<Message> iterate(int batchSize, long maxWaitMillis) {
        return active.iterate(batchSize, maxWaitMillis);
    }

    @Override
    public ConsumerInfo getConsumerInfo() throws IOException, JetStreamApiException {
        return active.getConsumerInfo();
    }

    @Override
    public Message nextMessage(Duration timeout) throws InterruptedException, IllegalStateException {
        return checkForOutOfOrder(active.nextMessage(timeout));
    }

    int x = 0;
    @Override
    public Message nextMessage(long timeoutMillis) throws InterruptedException, IllegalStateException {
        return checkForOutOfOrder(active.nextMessage(timeoutMillis));
    }

    @Override
    public String getSID() {
        return active.getSID();
    }

    @Override
    public String getSubject() {
        return active.getSubject();
    }

    @Override
    public String getQueueName() {
        return active.getQueueName();
    }

    @Override
    public Dispatcher getDispatcher() {
        return active.getDispatcher();
    }

    @Override
    public void unsubscribe() {
        active.unsubscribe();
    }

    @Override
    public Subscription unsubscribe(int after) {
        return active.unsubscribe(after);
    }

    @Override
    public void setPendingLimits(long maxMessages, long maxBytes) {
        active.setPendingLimits(maxMessages, maxBytes);
    }

    @Override
    public long getPendingMessageLimit() {
        return active.getPendingMessageLimit();
    }

    @Override
    public long getPendingByteLimit() {
        return active.getPendingByteLimit();
    }

    @Override
    public long getPendingMessageCount() {
        return active.getPendingMessageCount();
    }

    @Override
    public long getPendingByteCount() {
        return active.getPendingByteCount();
    }

    @Override
    public long getDeliveredCount() {
        return active.getDeliveredCount();
    }

    @Override
    public long getDroppedCount() {
        return active.getDroppedCount();
    }

    @Override
    public void clearDroppedCount() {
        active.clearDroppedCount();
    }

    @Override
    public CompletableFuture<Boolean> drain(Duration timeout) throws InterruptedException {
        return active.drain(timeout);
    }

    @Override
    public boolean isActive() {
        return active.isActive();
    }
}
