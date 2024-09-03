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

    NatsOrderedConsumerContext(NatsStreamContext streamContext, OrderedConsumerConfiguration config) throws JetStreamApiException {
        impl = new NatsConsumerContext(streamContext, null, config);
    }

    @Override
    public String getConsumerName() {
        return impl.getConsumerName();
    }

    @Override
    public Message next() throws IOException, InterruptedException, JetStreamStatusCheckedException, JetStreamApiException {
        return impl.next();
    }

    @Override
    public Message next(Duration maxWait) throws IOException, InterruptedException, JetStreamStatusCheckedException, JetStreamApiException {
        return impl.next(maxWait);
    }

    @Override
    public Message next(long maxWaitMillis) throws IOException, InterruptedException, JetStreamStatusCheckedException, JetStreamApiException {
        return impl.next(maxWaitMillis);
    }

    @Override
    public FetchConsumer fetchMessages(int maxMessages) throws IOException, JetStreamApiException {
        return impl.fetchMessages(maxMessages);
    }

    @Override
    public FetchConsumer fetchBytes(int maxBytes) throws IOException, JetStreamApiException {
        return impl.fetchBytes(maxBytes);
    }

    @Override
    public FetchConsumer fetch(FetchConsumeOptions fetchConsumeOptions) throws IOException, JetStreamApiException {
        return impl.fetch(fetchConsumeOptions);
    }

    @Override
    public IterableConsumer iterate() throws IOException, JetStreamApiException {
        return impl.iterate();
    }

    @Override
    public IterableConsumer iterate(ConsumeOptions consumeOptions) throws IOException, JetStreamApiException {
        return impl.iterate(consumeOptions);
    }

    @Override
    public MessageConsumer consume(MessageHandler handler) throws IOException, JetStreamApiException {
        return impl.consume(handler);
    }

    @Override
    public MessageConsumer consume(Dispatcher dispatcher, MessageHandler handler) throws IOException, JetStreamApiException {
        return impl.consume(dispatcher, handler);
    }

    @Override
    public MessageConsumer consume(ConsumeOptions consumeOptions, MessageHandler handler) throws IOException, JetStreamApiException {
        return impl.consume(consumeOptions, handler);
    }

    @Override
    public MessageConsumer consume(ConsumeOptions consumeOptions, Dispatcher dispatcher, MessageHandler handler) throws IOException, JetStreamApiException {
        return impl.consume(consumeOptions, dispatcher, handler);
    }
}
