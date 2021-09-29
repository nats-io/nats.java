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
import io.nats.client.SubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.support.JsonUtils;
import io.nats.client.support.NatsJetStreamConstants;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static io.nats.client.support.Validator.validatePullBatchSize;

/**
 * This is a JetStream specific subscription.
 */
public class NatsJetStreamSubscription extends NatsSubscription implements JetStreamSubscription, NatsJetStreamConstants {

    private final NatsJetStream js;
    private final boolean pullMode;

    private final String stream;
    private final String consumerName;
    private final String deliver;

    private final PullImpl pullImpl;
    private final NextMessageImpl nextMessageImpl;

    NatsJetStreamSubscription(String sid, String subject, String queueName,
                              NatsConnection connection, NatsDispatcher dispatcher,
                              NatsJetStream js, boolean pullMode, SubscribeOptions so,
                              String stream, String consumer, String deliver, ConsumerConfiguration consumerConfig) {
        super(sid, subject, queueName, connection, dispatcher);
        this.js = js;
        this.pullMode = pullMode;
        this.stream = stream;
        this.consumerName = consumer;
        this.deliver = pullMode ? null : deliver;

        // Provide a pull implementation to reduce runtime flag checking.
        // This implementation is called as a delegate in pull, pullNoWait and pullExpiresIn
        if (pullMode) {
            pullImpl = this::_pullImpl;
        }
        else {
            pullImpl = (batchSize, noWait, expiresIn) -> {
                throw new IllegalStateException("Subscription type does not allow pull.");
            };
        }

        // Provide a nextMessage implementation to reduce runtime flag checking
        // 'sync push' and 'pull' are allow to use nextMessage, they never have a dispatcher
        // async is always dispatched and not allowed to call nextMessage
        if (dispatcher == null) {
            // additionally optimize this implementation as sometimes the pre-processor is not called for
            NatsJetStreamMessagePreProcessor pre =
                new NatsJetStreamMessagePreProcessor(connection, so, consumerConfig, this, queueName != null, true);
            nextMessageImpl = pre.isNoOp() ? super::nextMessage : new PreProcessorNextMessageImpl(pre);
        }
        else {
            nextMessageImpl = timeout -> {
                throw new IllegalStateException("Calling nextMessage not allowed for async push.");
            };
        }
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
        return pullMode;
    }

    @Override
    public Message nextMessage(Duration timeout) throws InterruptedException, IllegalStateException {
        return nextMessageImpl.nextMessage(timeout);
    }

    interface NextMessageImpl {
        Message nextMessage(Duration timeout) throws InterruptedException, IllegalStateException;
    }

    class PreProcessorNextMessageImpl implements NextMessageImpl {
        final NatsJetStreamMessagePreProcessor pre;

        public PreProcessorNextMessageImpl(NatsJetStreamMessagePreProcessor pre) {
            this.pre = pre;
        }

        @Override
        public Message nextMessage(Duration timeout) throws InterruptedException, IllegalStateException {
            // null timeout is allowed, it means don't wait, messages must already be available.
            // so only process 1 message max
            if (timeout == null || timeout.isNegative()) {
                Message msg = NatsJetStreamSubscription.super.nextMessage(timeout);
                return msg == null || pre.preProcess(msg) ? null : msg;
            }

            // zero timeout means wait for ever,
            // so we process all messages until a regular message is received
            if (timeout.isZero()) {
                // loop through all messages until we get to a non protocol message
                // by using timeout of null, we never wait for messages, just read
                // messages that are already queued
                Message msg = NatsJetStreamSubscription.super.nextMessage(timeout);
                while (msg != null && pre.preProcess(msg)) {
                    msg = NatsJetStreamSubscription.super.nextMessage(timeout);
                }
                return msg;
            }

            // positive timeout, process as many messages we can in that time period
            long end = System.currentTimeMillis() + timeout.toMillis();
            Message msg = NatsJetStreamSubscription.super.nextMessage(timeout);
            while (msg != null && pre.preProcess(msg)) {
                long millis = end - System.currentTimeMillis();
                if (millis > 0) {
                    msg = NatsJetStreamSubscription.super.nextMessage(Duration.ofMillis(millis));
                }
                else {
                    msg = null;
                }
            }
            return msg;
        }
    };

    /**
     * {@inheritDoc}
     */
    @Override
    public void pull(int batchSize) {
        pullImpl.pull(batchSize, false, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullNoWait(int batchSize) {
        pullImpl.pull(batchSize, true, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullExpiresIn(int batchSize, Duration expiresIn) {
        pullImpl.pull(batchSize, false, expiresIn);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullExpiresIn(int batchSize, long expiresInMillis) {
        pullImpl.pull(batchSize, false, Duration.ofMillis(expiresInMillis));
    }

    interface PullImpl {
        void pull(int batchSize, boolean noWait, Duration expiresIn);
    }

    private void _pullImpl(int batchSize, boolean noWait, Duration expiresIn) {
        int batch = validatePullBatchSize(batchSize);
        String publishSubject = js.prependPrefix(String.format(JSAPI_CONSUMER_MSG_NEXT, stream, consumerName));
        connection.publish(publishSubject, getSubject(), getPullJson(batch, noWait, expiresIn));
        connection.lenientFlushBuffer();
    }

    byte[] getPullJson(int batch, boolean noWait, Duration expiresIn) {
        StringBuilder sb = JsonUtils.beginJson();
        JsonUtils.addField(sb, "batch", batch);
        JsonUtils.addFldWhenTrue(sb, "no_wait", noWait);
        JsonUtils.addFieldAsNanos(sb, "expires", expiresIn);
        return JsonUtils.endJson(sb).toString().getBytes(StandardCharsets.US_ASCII);
    }

    private static final Duration SUBSEQUENT_WAITS = Duration.ofMillis(500);

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Message> fetch(int batchSize, long maxWaitMillis) {
        return fetch(batchSize, Duration.ofMillis(maxWaitMillis));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Message> fetch(int batchSize, Duration maxWait) {
        List<Message> messages = new ArrayList<>(batchSize);

        try {
            pullNoWait(batchSize);
            read(batchSize, maxWait, messages);
            if (messages.size() == 0) {
                pullExpiresIn(batchSize, maxWait.minusMillis(10));
                read(batchSize, maxWait, messages);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return messages;
    }

    private void read(int batchSize, Duration maxWait, List<Message> messages) throws InterruptedException {
        Message msg = nextMessage(maxWait);
        while (msg != null) {
            if (msg.isJetStream()) {
                messages.add(msg);
                if (messages.size() == batchSize) {
                    break;
                }
            }
            msg = nextMessage(SUBSEQUENT_WAITS);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Message> iterate(final int batchSize, long maxWaitMillis) {
        return iterate(batchSize, Duration.ofMillis(maxWaitMillis));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Message> iterate(int batchSize, Duration maxWait) {
        pullNoWait(batchSize);

        return new Iterator<Message>() {
            int received = 0;
            boolean finished = false;
            boolean stepDown = true;
            Duration wait = maxWait;
            Message msg = null;

            @Override
            public boolean hasNext() {
                while (!finished && msg == null) {
                    try {
                        msg = nextMessage(wait);
                        wait = SUBSEQUENT_WAITS;
                        if (msg == null) {
                            if (received == 0 && stepDown) {
                                stepDown = false;
                                pullExpiresIn(batchSize, maxWait.minusMillis(10));
                            }
                            else {
                                finished = true;
                            }
                        }
                        else if (msg.isJetStream()) {
                            finished = ++received == batchSize;
                        }
                        else {
                            msg = null;
                        }
                    } catch (InterruptedException e) {
                        msg = null;
                        finished = true;
                        Thread.currentThread().interrupt();
                    }
                }
                return msg != null;
            }

            @Override
            public Message next() {
                Message next = msg;
                msg = null;
                return next;
            }
        };
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
                ", isPullMode=" + pullMode +
                '}';
    }
}
