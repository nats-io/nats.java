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
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static io.nats.client.impl.NatsJetStreamSubscription.SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL;

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

    private JetStreamSubscription current;
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

    void setCurrent(JetStreamSubscription sub) {
        this.current = sub;
        lastStreamSeq = -1;
        expectedConsumerSeq = 1; // always starts at 1
    }

    JetStreamSubscription getCurrent() {
        return current;
    }

    @Override
    public String toString() {
        if (current == null) {
            return "NatsJetStreamOrderedSubscription{" +
                "this=" + hashCode() +
                ", delegate=inactive" +
                ", stream='" + stream + '\'' +
                '}';
        }
        return "NatsJetStreamOrderedSubscription{" +
            "this=" + hashCode() +
            ", delegate=" + current.hashCode() +
            ", consumer='" + ((NatsJetStreamSubscription) current).getConsumerName() + '\'' +
            ", stream='" + stream + '\'' +
            ", deliver='" + ((NatsJetStreamSubscription) current).getDeliverSubject() + '\'' +
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

    @Override
    public Message nextMessage(Duration timeout) throws InterruptedException, IllegalStateException {
        return current == null ? null : checkForOutOfOrder(current.nextMessage(timeout));
    }

    @Override
    public Message nextMessage(long timeoutMillis) throws InterruptedException, IllegalStateException {
        return current == null ? null : checkForOutOfOrder(current.nextMessage(timeoutMillis));
    }

    private Message checkForOutOfOrder(Message msg) {
        if (msg != null) {
            long receivedConsumerSeq = msg.metaData().consumerSequence();
            if (expectedConsumerSeq != receivedConsumerSeq) {
                try {
                    if (dispatcher == null) {
                        current.unsubscribe();
                    }
                    else {
                        dispatcher.unsubscribe(current);
                    }
                    js.conn.lenientFlushBuffer();
                } catch (RuntimeException re) {
                    js.conn.processException(re);
                } finally {
                    current = null;
                }

                ConsumerConfiguration userCC = ConsumerConfiguration.builder(serverCC)
                    .deliverPolicy(DeliverPolicy.ByStartSequence)
                    .deliverSubject(null)
                    .startSequence(lastStreamSeq + 1)
                    .build();
                try {
                    current = js.finishCreateSubscription(subject, dispatcher, userHandler,
                        isAutoAck, false, so, stream, null,
                        userCC, null, null, null, this);
                } catch (Exception e) {
                    current = null;
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

    @Override
    public ConsumerInfo getConsumerInfo() throws IOException, JetStreamApiException {
        return current == null ? null : current.getConsumerInfo();
    }

    @Override
    public String getSID() {
        return guarded(() -> current.getSID(), String.class);
    }

    @Override
    public String getSubject() {
        return guarded(() -> current.getSubject(), String.class);
    }

    @Override
    public String getQueueName() {
        return guarded(() -> current.getQueueName(), String.class);
    }

    @Override
    public Dispatcher getDispatcher() {
        return guarded(() -> current.getDispatcher(), Dispatcher.class);
    }

    @Override
    public void unsubscribe() {
        guarded(() -> current.unsubscribe());
    }

    @Override
    public Subscription unsubscribe(int after) {
        return guarded(() -> current.unsubscribe(after), Subscription.class);
    }

    @Override
    public void setPendingLimits(long maxMessages, long maxBytes) {
        guarded(() -> current.setPendingLimits(maxMessages, maxBytes));
    }

    @Override
    public long getPendingMessageLimit() {
        return guarded(() -> current.getPendingMessageLimit());
    }

    @Override
    public long getPendingByteLimit() {
        return guarded(() -> current.getPendingByteLimit());
    }

    @Override
    public long getPendingMessageCount() {
        return guarded(() -> current.getPendingMessageCount());
    }

    @Override
    public long getPendingByteCount() {
        return guarded(() -> current.getPendingByteCount());
    }

    @Override
    public long getDeliveredCount() {
        return guarded(() -> current.getDeliveredCount());
    }

    @Override
    public long getDroppedCount() {
        return guarded(() -> current.getDroppedCount());
    }

    @Override
    public void clearDroppedCount() {
        guarded(() -> current.clearDroppedCount());
    }

    @Override
    public CompletableFuture<Boolean> drain(Duration timeout) throws InterruptedException {
        return current == null ? null: current.drain(timeout);
    }

    @Override
    public boolean isActive() {
        return guarded(() -> current.isActive());
    }

    private <T> T guarded(Supplier<T> supplier, Class<T> type) {
        return current == null ? null: supplier.get();
    }

    private long guarded(LongSupplier supplier) {
        return current == null ? Long.MIN_VALUE : supplier.getAsLong();
    }

    private boolean guarded(BooleanSupplier supplier) {
        return current != null && supplier.getAsBoolean();
    }

    interface VoidSupplier {
        void get();
    }

    private void guarded(VoidSupplier supplier) {
        if (current != null) {
            supplier.get();
        }
    }
}
