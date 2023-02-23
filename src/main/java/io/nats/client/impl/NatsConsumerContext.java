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
import io.nats.client.support.Validator;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * TODO
 */
public class NatsConsumerContext implements ConsumerContext {
    private final NatsJetStream js;
    private final NatsJetStreamManagement jsm;
    private final String stream;
    private final String consumer;

    private NatsConsumerContext(NatsConnection connection, JetStreamOptions jsOptions, String stream,
                                String consumer, ConsumerConfiguration cc) throws IOException, JetStreamApiException {
        js = new NatsJetStream(connection, jsOptions);
        jsm = new NatsJetStreamManagement(connection, jsOptions);
        this.stream = stream;
        if (consumer == null) {
            // todo sff pull and have diff behavior for ephemeral
            boolean ephemeral = true;
            if (ephemeral) {
                // TODO does it have a name? If not make one.
                // update cc if necessary
            }
            ConsumerInfo ci = jsm.addOrUpdateConsumer(stream, cc);
            this.consumer = ci.getName();
        }
        else {
            this.consumer = consumer;
        }

        getConsumerInfo();
    }

    NatsConsumerContext(NatsConnection connection, JetStreamOptions jsOptions, String stream, String consumer) throws IOException, JetStreamApiException {
        this(connection, jsOptions, stream, Validator.required(consumer, "Consumer"), null);
    }

    NatsConsumerContext(NatsConnection connection, JetStreamOptions jsOptions, String stream, ConsumerConfiguration consumerConfiguration) throws IOException, JetStreamApiException {
        this(connection, jsOptions, stream, null,
            Validator.required(consumerConfiguration, "Consumer Configuration"));
    }

    public String getName() {
        return consumer;
    }

    public ConsumerInfo getConsumerInfo() throws IOException, JetStreamApiException {
        return jsm.getConsumerInfo(stream, consumer);
    }

    private ConsumeOptions orDefault(ConsumeOptions consumeOptions) {
        return consumeOptions == null ? ConsumeOptions.DEFAULT_OPTIONS : consumeOptions;
    }

    @Override
    public List<Message> fetch(int count) throws IOException, JetStreamApiException {
        return fetch(count, null);
    }

    @Override
    public List<Message> fetch(int count, ConsumeOptions consumeOptions) throws IOException, JetStreamApiException {
        PullSubscribeOptions pso = PullSubscribeOptions.bind(stream, consumer);
        JetStreamSubscription sub = js.subscribe(null, pso);
        ConsumeOptions co = orDefault(consumeOptions);
        return sub.fetch(count, co.getExpiresIn());
    }

    @Override
    public Iterator<Message> iterate(int count) throws IOException, JetStreamApiException {
        return iterate(count, null);
    }

    @Override
    public Iterator<Message> iterate(int count, ConsumeOptions consumeOptions) throws IOException, JetStreamApiException {
        PullSubscribeOptions pso = PullSubscribeOptions.bind(stream, consumer);
        JetStreamSubscription sub = js.subscribe(null, pso);
        ConsumeOptions co = orDefault(consumeOptions);
        return sub.iterate(count, co.getExpiresIn());
    }

    @Override
    public MessageNextConsumer consume() throws IOException, JetStreamApiException {
        return consume((ConsumeOptions)null);
    }

    @Override
    public MessageNextConsumer consume(ConsumeOptions consumeOptions) throws IOException, JetStreamApiException {
        PullSubscribeOptions pso = PullSubscribeOptions.bind(stream, consumer);
        NatsJetStreamPullSubscription sub = (NatsJetStreamPullSubscription)js.subscribe(null, pso);
        ConsumeOptions co = orDefault(consumeOptions);
        return new NatsMessageNextConsumer(sub, co);
    }

    @Override
    public MessageConsumer consume(MessageHandler handler) throws IOException, JetStreamApiException {
        return null;
    }

    @Override
    public MessageConsumer consume(MessageHandler handler, ConsumeOptions consumeOptions) throws IOException, JetStreamApiException {
        PullSubscribeOptions pso = PullSubscribeOptions.bind(stream, consumer);
        NatsJetStreamPullSubscription sub = (NatsJetStreamPullSubscription)js.subscribe(null, pso);
        ConsumeOptions co = orDefault(consumeOptions);
        return new NatsMessageNextConsumer(sub, co);
    }
}
