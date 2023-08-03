// Copyright 2020-2023 The NATS Authors
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
import io.nats.client.api.OrderedConsumerConfiguration;
import io.nats.client.support.Validator;

import java.io.IOException;
import java.time.Duration;

import static io.nats.client.BaseConsumeOptions.DEFAULT_EXPIRES_IN_MILLIS;
import static io.nats.client.BaseConsumeOptions.MIN_EXPIRES_MILLS;
import static io.nats.client.ConsumeOptions.DEFAULT_CONSUME_OPTIONS;
import static io.nats.client.impl.NatsJetStreamSubscription.EXPIRE_ADJUSTMENT;
import static io.nats.client.support.ConsumerUtils.nextOrderedConsumerConfiguration;

/**
 * SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
 */
public class NatsConsumerContext implements ConsumerContext, SimplifiedSubscriptionMaker {
    private enum ConsumeType {
        next, fetch, iterate, consume
    }

    private final Object stateLock;
    private final NatsStreamContext streamContext;
    private final boolean ordered;
    private final String consumerName;
    private final ConsumerConfiguration originalOrderedCc;
    private final String subscribeSubject;
    private final PullSubscribeOptions unorderedBindPso;

    private ConsumerInfo cachedConsumerInfo;
    private NatsMessageConsumerBase lastConsumer;
    private long highestSeq;
    private ConsumeType lastConsumerType;
    private Dispatcher dispatcher;

    NatsConsumerContext(NatsStreamContext streamContext, ConsumerInfo ci) {
        stateLock = new Object();
        this.streamContext = streamContext;
        ordered = false;
        consumerName = ci.getName();
        originalOrderedCc = null;
        subscribeSubject = null;
        unorderedBindPso = PullSubscribeOptions.bind(streamContext.streamName, consumerName);
        cachedConsumerInfo = ci;
    }

    NatsConsumerContext(NatsStreamContext streamContext, OrderedConsumerConfiguration config) {
        stateLock = new Object();
        this.streamContext = streamContext;
        ordered = true;
        consumerName = null;
        originalOrderedCc = ConsumerConfiguration.builder()
            .filterSubject(config.getFilterSubject())
            .deliverPolicy(config.getDeliverPolicy())
            .startSequence(config.getStartSequence())
            .startTime(config.getStartTime())
            .replayPolicy(config.getReplayPolicy())
            .headersOnly(config.getHeadersOnly())
            .build();
        subscribeSubject = originalOrderedCc.getFilterSubject();
        unorderedBindPso = null;
    }

    static class OrderedPullSubscribeOptionsBuilder extends PullSubscribeOptions.Builder {
        public OrderedPullSubscribeOptionsBuilder(String streamName, ConsumerConfiguration cc) {
            stream(streamName);
            configuration(cc);
            ordered = true;
        }
    }

    public NatsJetStreamPullSubscription subscribe(MessageHandler messageHandler) throws IOException, JetStreamApiException {
        PullSubscribeOptions pso;
        if (ordered) {
            if (lastConsumer != null) {
                highestSeq = Math.max(highestSeq, lastConsumer.pmm.lastStreamSeq);
            }
            ConsumerConfiguration cc = lastConsumer == null
                ? originalOrderedCc
                : nextOrderedConsumerConfiguration(originalOrderedCc, highestSeq, null);
            pso = new OrderedPullSubscribeOptionsBuilder(streamContext.streamName, cc).build();
        }
        else {
            pso = unorderedBindPso;
        }

        if (messageHandler == null) {
            return (NatsJetStreamPullSubscription)streamContext.js.subscribe(subscribeSubject, pso);
        }

        if (dispatcher == null) {
            dispatcher = streamContext.js.conn.createDispatcher();
        }
        return (NatsJetStreamPullSubscription)streamContext.js.subscribe(subscribeSubject, dispatcher, messageHandler, pso);
    }

    private void checkState() throws IOException {
        if (lastConsumer != null) {
            if (ordered) {
                if (!lastConsumer.finished) {
                    throw new IOException(lastConsumerType + "is already running. Ordered Consumer does not allow multiple instances at time.");
                }
            }
            if (lastConsumer.finished) {
                lastConsumer.lenientClose(); // finished, might as well make sure the sub is closed.
            }
        }
    }

    private NatsMessageConsumerBase trackConsume(ConsumeType ct, NatsMessageConsumerBase con) {
        lastConsumerType = ct;
        lastConsumer = con;
        return con;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getConsumerName() {
        return consumerName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerInfo getConsumerInfo() throws IOException, JetStreamApiException {
        if (consumerName != null) {
            cachedConsumerInfo = streamContext.jsm.getConsumerInfo(streamContext.streamName, cachedConsumerInfo.getName());
        }
        return cachedConsumerInfo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerInfo getCachedConsumerInfo() {
        return cachedConsumerInfo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Message next() throws IOException, InterruptedException, JetStreamStatusCheckedException, JetStreamApiException {
        return next(DEFAULT_EXPIRES_IN_MILLIS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Message next(Duration maxWait) throws IOException, InterruptedException, JetStreamStatusCheckedException, JetStreamApiException {
        return maxWait == null ? next(DEFAULT_EXPIRES_IN_MILLIS) : next(maxWait.toMillis());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Message next(long maxWaitMillis) throws IOException, InterruptedException, JetStreamStatusCheckedException, JetStreamApiException {
        synchronized (stateLock) {
            checkState();
            if (maxWaitMillis < MIN_EXPIRES_MILLS) {
                throw new IllegalArgumentException("Max wait must be at least " + MIN_EXPIRES_MILLS + " milliseconds.");
            }

            NatsMessageConsumerBase c = new NatsMessageConsumerBase(cachedConsumerInfo, subscribe(null));
            trackConsume(ConsumeType.next, c);
            c.sub._pull(PullRequestOptions.builder(1).expiresIn(maxWaitMillis - EXPIRE_ADJUSTMENT).build(), false, null);

            try {
                return c.sub.nextMessage(maxWaitMillis);
            }
            catch (JetStreamStatusException e) {
                throw new JetStreamStatusCheckedException(e);
            }
            finally {
                c.lenientClose();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FetchConsumer fetchMessages(int maxMessages) throws IOException, JetStreamApiException {
        return fetch(FetchConsumeOptions.builder().maxMessages(maxMessages).build());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FetchConsumer fetchBytes(int maxBytes) throws IOException, JetStreamApiException {
        return fetch(FetchConsumeOptions.builder().maxBytes(maxBytes).build());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FetchConsumer fetch(FetchConsumeOptions fetchConsumeOptions) throws IOException, JetStreamApiException {
        synchronized (stateLock) {
            checkState();
            Validator.required(fetchConsumeOptions, "Fetch Consume Options");
            return (FetchConsumer)trackConsume(ConsumeType.fetch, new NatsFetchConsumer(this, cachedConsumerInfo, fetchConsumeOptions));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IterableConsumer iterate() throws IOException, JetStreamApiException {
        return iterate(DEFAULT_CONSUME_OPTIONS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IterableConsumer iterate(ConsumeOptions consumeOptions) throws IOException, JetStreamApiException {
        synchronized (stateLock) {
            checkState();
            Validator.required(consumeOptions, "Consume Options");
            return (IterableConsumer) trackConsume(ConsumeType.iterate, new NatsIterableConsumer(this, cachedConsumerInfo, consumeOptions));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageConsumer consume(MessageHandler handler) throws IOException, JetStreamApiException {
        return consume(handler, DEFAULT_CONSUME_OPTIONS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageConsumer consume(MessageHandler handler, ConsumeOptions consumeOptions) throws IOException, JetStreamApiException {
        synchronized (stateLock) {
            checkState();
            Validator.required(handler, "Message Handler");
            Validator.required(consumeOptions, "Consume Options");
            return trackConsume(ConsumeType.consume, new NatsMessageConsumer(this, cachedConsumerInfo, handler, consumeOptions));
        }
    }
}
