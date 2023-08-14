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

/**
 * SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
 */
public class NatsConsumerContext implements ConsumerContext, SimplifiedSubscriptionMaker {
    private final Object stateLock;
    private final NatsStreamContext streamCtx;
    private final boolean ordered;
    private final String consumerName;
    private final ConsumerConfiguration originalOrderedCc;
    private final String subscribeSubject;
    private final PullSubscribeOptions unorderedBindPso;

    private ConsumerInfo cachedConsumerInfo;
    private NatsMessageConsumerBase lastConsumer;
    private long highestSeq;
    private Dispatcher defaultDispatcher;

    NatsConsumerContext(NatsStreamContext sc, ConsumerInfo ci) {
        stateLock = new Object();
        streamCtx = sc;
        ordered = false;
        consumerName = ci.getName();
        originalOrderedCc = null;
        subscribeSubject = null;
        unorderedBindPso = PullSubscribeOptions.bind(sc.streamName, consumerName);
        cachedConsumerInfo = ci;
    }

    NatsConsumerContext(NatsStreamContext sc, OrderedConsumerConfiguration config) {
        stateLock = new Object();
        streamCtx = sc;
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
        OrderedPullSubscribeOptionsBuilder(String streamName, ConsumerConfiguration cc) {
            stream(streamName);
            configuration(cc);
            ordered = true;
        }
    }

    public NatsJetStreamPullSubscription subscribe(MessageHandler messageHandler, Dispatcher userDispatcher) throws IOException, JetStreamApiException {
        PullSubscribeOptions pso;
        if (ordered) {
            if (lastConsumer != null) {
                highestSeq = Math.max(highestSeq, lastConsumer.pmm.lastStreamSeq);
            }
            ConsumerConfiguration cc = lastConsumer == null
                ? originalOrderedCc
                : streamCtx.js.nextOrderedConsumerConfiguration(originalOrderedCc, highestSeq, null);
            pso = new OrderedPullSubscribeOptionsBuilder(streamCtx.streamName, cc).build();
        }
        else {
            pso = unorderedBindPso;
        }

        if (messageHandler == null) {
            return (NatsJetStreamPullSubscription) streamCtx.js.subscribe(subscribeSubject, pso);
        }

        Dispatcher d = userDispatcher;
        if (d == null) {
            if (defaultDispatcher == null) {
                defaultDispatcher = streamCtx.js.conn.createDispatcher();
            }
            d = defaultDispatcher;
        }
        return (NatsJetStreamPullSubscription) streamCtx.js.subscribe(subscribeSubject, d, messageHandler, pso);
    }

    private void checkState() throws IOException {
        if (lastConsumer != null) {
            if (ordered) {
                if (!lastConsumer.finished.get()) {
                    throw new IOException("The ordered consumer is already receiving messages. Ordered Consumer does not allow multiple instances at time.");
                }
            }
            if (lastConsumer.finished.get() && !lastConsumer.stopped.get()) {
                lastConsumer.lenientClose(); // finished, might as well make sure the sub is closed.
            }
        }
    }

    private NatsMessageConsumerBase trackConsume(NatsMessageConsumerBase con) {
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
            cachedConsumerInfo = streamCtx.jsm.getConsumerInfo(streamCtx.streamName, cachedConsumerInfo.getName());
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
        NatsMessageConsumerBase con;
        synchronized (stateLock) {
            checkState();
            if (maxWaitMillis < MIN_EXPIRES_MILLS) {
                throw new IllegalArgumentException("Max wait must be at least " + MIN_EXPIRES_MILLS + " milliseconds.");
            }

            con = new NatsMessageConsumerBase(cachedConsumerInfo);
            con.initSub(subscribe(null, null));
            con.sub._pull(PullRequestOptions.builder(1)
                .expiresIn(maxWaitMillis - EXPIRE_ADJUSTMENT)
                .build(), false, null);
            trackConsume(con);
        }

        // intentionally outside of lock
        try {
            return con.sub.nextMessage(maxWaitMillis);
        }
        finally {
            try {
                con.finished.set(true);
                con.close();
            }
            catch (Exception e) {
                // from close/autocloseable, but we know it doesn't actually throw
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
            return (FetchConsumer)trackConsume(new NatsFetchConsumer(this, cachedConsumerInfo, fetchConsumeOptions));
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
            return (IterableConsumer) trackConsume(new NatsIterableConsumer(this, cachedConsumerInfo, consumeOptions));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageConsumer consume(MessageHandler handler) throws IOException, JetStreamApiException {
        return consume(DEFAULT_CONSUME_OPTIONS, null, handler);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageConsumer consume(Dispatcher dispatcher, MessageHandler handler) throws IOException, JetStreamApiException {
        return consume(DEFAULT_CONSUME_OPTIONS, dispatcher, handler);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageConsumer consume(ConsumeOptions consumeOptions, MessageHandler handler) throws IOException, JetStreamApiException {
        return consume(consumeOptions, null, handler);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageConsumer consume(ConsumeOptions consumeOptions, Dispatcher userDispatcher, MessageHandler handler) throws IOException, JetStreamApiException {
        synchronized (stateLock) {
            checkState();
            Validator.required(handler, "Message Handler");
            Validator.required(consumeOptions, "Consume Options");
            return trackConsume(new NatsMessageConsumer(this, cachedConsumerInfo, consumeOptions, userDispatcher, handler));
        }
    }
}
