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
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.time.Duration;

/**
 * Implementation of Ordered Consumer Context
 */
public class NatsOrderedConsumerContext implements OrderedConsumerContext {
    private final NatsConsumerContext impl;

    NatsOrderedConsumerContext(@NonNull NatsStreamContext streamContext,
                               @NonNull OrderedConsumerConfiguration config) {
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
    @Nullable
    public Message next() throws IOException, InterruptedException, JetStreamStatusCheckedException, JetStreamApiException {
        return impl.next();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nullable
    public Message next(@Nullable Duration maxWait) throws IOException, InterruptedException, JetStreamStatusCheckedException, JetStreamApiException {
        return impl.next(maxWait);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nullable
    public Message next(long maxWaitMillis) throws IOException, InterruptedException, JetStreamStatusCheckedException, JetStreamApiException {
        return impl.next(maxWaitMillis);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public FetchConsumer fetchMessages(int maxMessages) throws IOException, JetStreamApiException {
        return impl.fetchMessages(maxMessages);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public FetchConsumer fetchBytes(int maxBytes) throws IOException, JetStreamApiException {
        return impl.fetchBytes(maxBytes);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public FetchConsumer fetch(@NonNull FetchConsumeOptions fetchConsumeOptions) throws IOException, JetStreamApiException {
        return impl.fetch(fetchConsumeOptions);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public IterableConsumer iterate() throws IOException, JetStreamApiException {
        return impl.iterate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public IterableConsumer iterate(@NonNull ConsumeOptions consumeOptions) throws IOException, JetStreamApiException {
        return impl.iterate(consumeOptions);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public MessageConsumer consume(@NonNull MessageHandler handler) throws IOException, JetStreamApiException {
        return impl.consume(handler);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public MessageConsumer consume(@Nullable Dispatcher dispatcher, @NonNull MessageHandler handler) throws IOException, JetStreamApiException {
        return impl.consume(dispatcher, handler);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public MessageConsumer consume(@NonNull ConsumeOptions consumeOptions, @NonNull MessageHandler handler) throws IOException, JetStreamApiException {
        return impl.consume(consumeOptions, handler);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NonNull
    public MessageConsumer consume(@NonNull ConsumeOptions consumeOptions, @Nullable Dispatcher dispatcher, @NonNull MessageHandler handler) throws IOException, JetStreamApiException {
        return impl.consume(consumeOptions, dispatcher, handler);
    }
}
