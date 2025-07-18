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
import io.nats.client.api.OrderedConsumerConfiguration;

import java.io.IOException;
import java.time.Duration;

/**
 * Implementation of Ordered Consumer Context
 */
public class NatsOrderedConsumerContext implements OrderedConsumerContext {
    private final NatsConsumerContext impl;

    NatsOrderedConsumerContext(NatsStreamContext streamContext, OrderedConsumerConfiguration config) {
        impl = new NatsConsumerContext(streamContext, null, config);
    }

    /**
     * Gets the consumer name created for the underlying Ordered Consumer
     * This will return null until the first consume (next, iterate, fetch, consume)
     * is executed because the JetStream consumer, which carries the name,
     * has not been created yet.
     * <p>
     * The consumer name is subject to change for 2 reasons.
     * 1. Any time next(...) is called
     * 2. Anytime a message is received out of order for instance because of a disconnection
     * </p>
     * <p>If your OrderedConsumerConfiguration has a consumerNamePrefix,
     * the consumer name will always start with the prefix
     * </p>
     * @return the consumer name or null
     */
    @Override
    public String getConsumerName() {
        return impl.getConsumerName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Message next() throws IOException, InterruptedException, JetStreamStatusCheckedException, JetStreamApiException {
        return impl.next();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Message next(Duration maxWait) throws IOException, InterruptedException, JetStreamStatusCheckedException, JetStreamApiException {
        return impl.next(maxWait);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Message next(long maxWaitMillis) throws IOException, InterruptedException, JetStreamStatusCheckedException, JetStreamApiException {
        return impl.next(maxWaitMillis);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FetchConsumer fetchMessages(int maxMessages) throws IOException, JetStreamApiException {
        return impl.fetchMessages(maxMessages);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FetchConsumer fetchBytes(int maxBytes) throws IOException, JetStreamApiException {
        return impl.fetchBytes(maxBytes);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FetchConsumer fetch(FetchConsumeOptions fetchConsumeOptions) throws IOException, JetStreamApiException {
        return impl.fetch(fetchConsumeOptions);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IterableConsumer iterate() throws IOException, JetStreamApiException {
        return impl.iterate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IterableConsumer iterate(ConsumeOptions consumeOptions) throws IOException, JetStreamApiException {
        return impl.iterate(consumeOptions);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageConsumer consume(MessageHandler handler) throws IOException, JetStreamApiException {
        return impl.consume(handler);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageConsumer consume(Dispatcher dispatcher, MessageHandler handler) throws IOException, JetStreamApiException {
        return impl.consume(dispatcher, handler);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageConsumer consume(ConsumeOptions consumeOptions, MessageHandler handler) throws IOException, JetStreamApiException {
        return impl.consume(consumeOptions, handler);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageConsumer consume(ConsumeOptions consumeOptions, Dispatcher dispatcher, MessageHandler handler) throws IOException, JetStreamApiException {
        return impl.consume(consumeOptions, dispatcher, handler);
    }
}
