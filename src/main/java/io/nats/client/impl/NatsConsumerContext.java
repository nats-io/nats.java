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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import static io.nats.client.BaseConsumeOptions.DEFAULT_EXPIRES_IN_MILLIS;
import static io.nats.client.BaseConsumeOptions.MIN_EXPIRES_MILLS;
import static io.nats.client.ConsumeOptions.DEFAULT_CONSUME_OPTIONS;
import static io.nats.client.impl.NatsJetStreamSubscription.EXPIRE_ADJUSTMENT;

/**
 * Implementation of Consumer Context
 */
public class NatsConsumerContext implements ConsumerContext, SimplifiedSubscriptionMaker {
    private final ReentrantLock stateLock;
    private final NatsStreamContext streamCtx;
    private final boolean ordered;
    private final ConsumerConfiguration orderedConsumerConfigTemplate;
    private final String orderedConsumerNamePrefix;
    private final PullSubscribeOptions unorderedBindPso;

    private final AtomicReference<ConsumerInfo> cachedConsumerInfo;
    private final AtomicReference<String> consumerName;
    private final AtomicLong highestSeq;
    private final AtomicReference<Dispatcher> defaultDispatcher;
    private final AtomicReference<NatsMessageConsumerBase> lastConsumer;

    NatsConsumerContext(NatsStreamContext sc, ConsumerInfo unorderedConsumerInfo, OrderedConsumerConfiguration occ) {
        stateLock = new ReentrantLock();
        streamCtx = sc;
        cachedConsumerInfo = new AtomicReference<>();
        consumerName = new AtomicReference<>();
        highestSeq = new AtomicLong();
        defaultDispatcher = new AtomicReference<>();
        lastConsumer = new AtomicReference<>();
        if (unorderedConsumerInfo != null) {
            ordered = false;
            orderedConsumerNamePrefix = null;
            orderedConsumerConfigTemplate = null;
            cachedConsumerInfo.set(unorderedConsumerInfo);
            consumerName.set(unorderedConsumerInfo.getName());
            unorderedBindPso = PullSubscribeOptions.fastBind(sc.streamName, unorderedConsumerInfo.getName());
        }
        else {
            ordered = true;
            orderedConsumerNamePrefix = occ.getConsumerNamePrefix();
            orderedConsumerConfigTemplate = ConsumerConfiguration.builder()
                .filterSubjects(occ.getFilterSubjects())
                .deliverPolicy(occ.getDeliverPolicy())
                .startSequence(occ.getStartSequence())
                .startTime(occ.getStartTime())
                .replayPolicy(occ.getReplayPolicy())
                .headersOnly(occ.getHeadersOnly())
                .build();
            unorderedBindPso = null;
        }
    }

    static class OrderedPullSubscribeOptionsBuilder extends PullSubscribeOptions.Builder {
        OrderedPullSubscribeOptionsBuilder(String streamName, ConsumerConfiguration cc) {
            stream(streamName);
            configuration(cc);
            ordered = true;
        }
    }

    int x = 0;
    @Override
    public NatsJetStreamPullSubscription subscribe(MessageHandler messageHandler, Dispatcher userDispatcher, PullMessageManager optionalPmm, Long optionalInactiveThreshold) throws IOException, JetStreamApiException {
        PullSubscribeOptions pso;
        if (ordered) {
            NatsMessageConsumerBase lastCon = lastConsumer.get();
            if (lastCon != null) {
                highestSeq.set(Math.max(highestSeq.get(), lastCon.pmm.lastStreamSeq));
            }
            consumerName.set(orderedConsumerNamePrefix == null ? null : orderedConsumerNamePrefix + NUID.nextGlobalSequence());
            ConsumerConfiguration cc = streamCtx.js.consumerConfigurationForOrdered(
                orderedConsumerConfigTemplate, highestSeq.get(), null, consumerName.get(), optionalInactiveThreshold);
            pso = new OrderedPullSubscribeOptionsBuilder(streamCtx.streamName, cc).build();
        }
        else {
            pso = unorderedBindPso;
        }

        if (messageHandler == null) {
            return (NatsJetStreamPullSubscription) streamCtx.js.createSubscription(
                null, null, pso, null, null, null, false, optionalPmm);
        }

        Dispatcher d = userDispatcher;
        if (d == null) {
            d = defaultDispatcher.get();
            if (d == null) {
                d = streamCtx.js.conn.createDispatcher();
                defaultDispatcher.set(d);
            }
        }
        return (NatsJetStreamPullSubscription) streamCtx.js.createSubscription(
            null, null, pso, null, (NatsDispatcher) d, messageHandler, false, optionalPmm);
    }

    private void checkState() throws IOException {
        NatsMessageConsumerBase lastCon = lastConsumer.get();
        if (lastCon != null) {
            if (ordered) {
                if (!lastCon.finished.get()) {
                    throw new IOException("The ordered consumer is already receiving messages. Ordered Consumer does not allow multiple instances at time.");
                }
            }
            if (lastCon.finished.get() && !lastCon.stopped.get()) {
                lastCon.lenientClose(); // finished, might as well make sure the sub is closed.
            }
        }
    }

    private NatsMessageConsumerBase trackConsume(NatsMessageConsumerBase con) {
        lastConsumer.set(con);
        return con;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getConsumerName() {
        return consumerName.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerInfo getConsumerInfo() throws IOException, JetStreamApiException {
        ConsumerInfo ci = streamCtx.jsm.getConsumerInfo(streamCtx.streamName, consumerName.get());
        cachedConsumerInfo.set(ci);
        consumerName.set(ci.getName());
        return ci;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerInfo getCachedConsumerInfo() {
        return cachedConsumerInfo.get();
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
        if (maxWaitMillis < MIN_EXPIRES_MILLS) {
            throw new IllegalArgumentException("Max wait must be at least " + MIN_EXPIRES_MILLS + " milliseconds.");
        }

        NatsMessageConsumerBase nmcb = null;
        try {
            stateLock.lock();
            checkState();

            try {
                long inactiveThreshold = maxWaitMillis * 110 / 100; // 10% longer than the wait
                nmcb = new NatsMessageConsumerBase(consumerName.get(), cachedConsumerInfo.get());
                nmcb.initSub(subscribe(null, null, null, inactiveThreshold));
                nmcb.sub._pull(PullRequestOptions.builder(1)
                    .expiresIn(maxWaitMillis - EXPIRE_ADJUSTMENT)
                    .build(), false, null);
                trackConsume(nmcb);
            }
            catch (Exception e) {
                if (nmcb != null) {
                    try {
                        nmcb.close();
                    }
                    catch (Exception ignore) {}
                }
                return null;
            }
        }
        finally {
            stateLock.unlock();
        }

        // intentionally outside of lock
        try {
            return nmcb.sub.nextMessage(maxWaitMillis);
        }
        finally {
            try {
                nmcb.finished.set(true);
                nmcb.close();
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
        try {
            stateLock.lock();
            checkState();
            Validator.required(fetchConsumeOptions, "Fetch Consume Options");
            return (FetchConsumer)trackConsume(new NatsFetchConsumer(this, consumerName.get(), cachedConsumerInfo.get(), fetchConsumeOptions));
        }
        finally {
            stateLock.unlock();
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
        try {
            stateLock.lock();
            checkState();
            Validator.required(consumeOptions, "Consume Options");
            return (IterableConsumer) trackConsume(new NatsIterableConsumer(this, consumerName.get(), cachedConsumerInfo.get(), consumeOptions));
        }
        finally {
            stateLock.unlock();
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
        try {
            stateLock.lock();
            checkState();
            Validator.required(handler, "Message Handler");
            Validator.required(consumeOptions, "Consume Options");
            return trackConsume(new NatsMessageConsumer(this, consumerName.get(), cachedConsumerInfo.get(), consumeOptions, userDispatcher, handler));
        }
        finally {
            stateLock.unlock();
        }
    }
}
