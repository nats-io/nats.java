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
import io.nats.client.api.ConsumerInfo;
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
public class NatsConsumerContext implements ConsumerContext {

    private final NatsStreamContext streamContext;
    private final NatsJetStream js;
    private final PullSubscribeOptions bindPso;
    private ConsumerInfo lastConsumerInfo;

    NatsConsumerContext(NatsStreamContext streamContext, ConsumerInfo ci) throws IOException {
        this.streamContext = streamContext;
        js = new NatsJetStream(streamContext.jsm.conn, streamContext.jsm.jso);
        bindPso = PullSubscribeOptions.bind(streamContext.streamName, ci.getName());
        lastConsumerInfo = ci;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getConsumerName() {
        return lastConsumerInfo.getName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerInfo getConsumerInfo() throws IOException, JetStreamApiException {
        lastConsumerInfo = streamContext.jsm.getConsumerInfo(streamContext.streamName, lastConsumerInfo.getName());
        return lastConsumerInfo;
    }

    @Override
    public ConsumerInfo getCachedConsumerInfo() {
        return lastConsumerInfo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Message next() throws IOException, InterruptedException, JetStreamStatusCheckedException, JetStreamApiException {
        return new NextSub(DEFAULT_EXPIRES_IN_MILLIS).next();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Message next(Duration maxWait) throws IOException, InterruptedException, JetStreamStatusCheckedException, JetStreamApiException {
        if (maxWait == null) {
            return new NextSub(DEFAULT_EXPIRES_IN_MILLIS).next();
        }
        return next(maxWait.toMillis());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Message next(long maxWaitMillis) throws IOException, InterruptedException, JetStreamStatusCheckedException, JetStreamApiException {
        if (maxWaitMillis < MIN_EXPIRES_MILLS) {
            throw new IllegalArgumentException("Max wait must be at least " + MIN_EXPIRES_MILLS + " milliseconds.");
        }
        return new NextSub(maxWaitMillis).next();
    }

    class NextSub {
        private final long maxWaitMillis;
        private final NatsJetStreamPullSubscription sub;

        public NextSub(long maxWaitMillis) throws JetStreamApiException, IOException {
            sub = new SubscriptionMaker().makeSubscription(null);
            this.maxWaitMillis = maxWaitMillis;
            sub._pull(PullRequestOptions.builder(1).expiresIn(maxWaitMillis - EXPIRE_ADJUSTMENT).build(), false, null);
        }

        Message next() throws JetStreamStatusCheckedException, InterruptedException {
            try {
                return sub.nextMessage(maxWaitMillis);
            }
            catch (JetStreamStatusException e) {
                throw new JetStreamStatusCheckedException(e);
            }
        }

        @Override
        protected void finalize() throws Throwable {
            {
                try {
                    sub.unsubscribe();
                }
                catch (Exception ignore) {
                    // ignored
                }
                super.finalize();
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
        Validator.required(fetchConsumeOptions, "Fetch Consume Options");
        return new NatsFetchConsumer(new SubscriptionMaker(), fetchConsumeOptions);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IterableConsumer consume() throws IOException, JetStreamApiException {
        return new NatsIterableConsumer(new SubscriptionMaker(), DEFAULT_CONSUME_OPTIONS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IterableConsumer consume(ConsumeOptions consumeOptions) throws IOException, JetStreamApiException {
        Validator.required(consumeOptions, "Consume Options");
        return new NatsIterableConsumer(new SubscriptionMaker(), consumeOptions);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageConsumer consume(MessageHandler handler) throws IOException, JetStreamApiException {
        Validator.required(handler, "Message Handler");
        return new NatsMessageConsumer(new SubscriptionMaker(), handler, DEFAULT_CONSUME_OPTIONS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageConsumer consume(MessageHandler handler, ConsumeOptions consumeOptions) throws IOException, JetStreamApiException {
        Validator.required(handler, "Message Handler");
        Validator.required(consumeOptions, "Consume Options");
        return new NatsMessageConsumer(new SubscriptionMaker(), handler, consumeOptions);
    }

    class SubscriptionMaker {
        Dispatcher dispatcher;

        public NatsJetStreamPullSubscription makeSubscription(MessageHandler messageHandler) throws IOException, JetStreamApiException {
            if (messageHandler == null) {
                return (NatsJetStreamPullSubscription)js.subscribe(null, bindPso);
            }

            dispatcher = js.conn.createDispatcher();
            return (NatsJetStreamPullSubscription)js.subscribe(null, dispatcher, messageHandler, bindPso);
        }
    }
}
