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
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.nats.client.ConsumeOptions.DEFAULT_FETCH_ALL_OPTIONS;
import static io.nats.client.ConsumeOptions.DEFAULT_OPTIONS;

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
        return consumeOptions == null ? DEFAULT_OPTIONS : consumeOptions;
    }

    private ConsumeOptions orDefaultFA(ConsumeOptions consumeOptions) {
        return consumeOptions == null ? DEFAULT_FETCH_ALL_OPTIONS : consumeOptions;
    }

    @Override
    public List<Message> fetchAll(int count) throws IOException, JetStreamApiException {
        return fetchAll(count, null);
    }

    @Override
    public List<Message> fetchAll(int count, ConsumeOptions consumeOptions) throws IOException, JetStreamApiException {
        PullSubscribeOptions pso = PullSubscribeOptions.bind(stream, consumer);
        JetStreamSubscription sub = js.subscribe(null, pso);
        ConsumeOptions co = orDefaultFA(consumeOptions);
        return sub.fetch(count, co.getExpiresIn());
    }

    static class InternalFetchConsumer extends NatsMessageConsumer implements FetchConsumer {
        Iterator<Message> iterator;
        boolean isSubbed = true;

        public InternalFetchConsumer(NatsJetStreamPullSubscription sub, ConsumeOptions co, int count) {
            super(sub, co);
            iterator = sub.iterate(count, co.getExpiresIn());
        }

        @Override
        public Message nextMessage() {
            if (iterator.hasNext()) {
                return iterator.next();
            }
            unsub();
            return null;
        }

        private synchronized void unsub() {
            if (isSubbed) {
                isSubbed = false;
                new Thread(this::unsubscribe).start();
            }
        }
    }

    @Override
    public FetchConsumer fetch(int count) throws IOException, JetStreamApiException {
        return fetch(count, null);
    }

    @Override
    public FetchConsumer fetch(int count, ConsumeOptions consumeOptions) throws IOException, JetStreamApiException {
        PullSubscribeOptions pso = PullSubscribeOptions.bind(stream, consumer);
        NatsJetStreamPullSubscription sub = (NatsJetStreamPullSubscription)js.subscribe(null, pso);
        return new InternalFetchConsumer(sub, orDefault(consumeOptions), count);
    }

    @Override
    public EndlessConsumer consume() throws IOException, JetStreamApiException {
        return consume((ConsumeOptions)null);
    }

    @Override
    public EndlessConsumer consume(ConsumeOptions consumeOptions) throws IOException, JetStreamApiException {
        PullSubscribeOptions pso = PullSubscribeOptions.bind(stream, consumer);
        NatsJetStreamPullSubscription sub = (NatsJetStreamPullSubscription)js.subscribe(null, pso);
        ConsumeOptions co = orDefault(consumeOptions);
        return new NatsEndlessConsumer(sub, co);
    }

    @Override
    public MessageConsumer consume(MessageHandler handler) throws IOException, JetStreamApiException {
        return null;
    }

    static class InternalMessageConsumer extends NatsMessageConsumer {
        NatsEndlessConsumer endlessConsumer;
        MessageHandler handler;
        Thread hack;
        AtomicBoolean keepRunning = new AtomicBoolean(true);

        public InternalMessageConsumer(NatsJetStreamPullSubscription sub, ConsumeOptions co,
                                       MessageHandler handler) {
            super(sub, co);

            endlessConsumer = new NatsEndlessConsumer(sub, co);
            this.handler = handler;
            hack = new Thread(() -> {
                while (keepRunning.get()) {
                    try {
                        Message m = endlessConsumer.nextMessage(co.getExpiresIn());
                        if (m != null) {
                            handler.onMessage(m);
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            hack.start();
        }

        @Override
        public void unsubscribe() {
            super.unsubscribe();
            keepRunning.set(false);
        }

        @Override
        public void unsubscribe(int after) {
            super.unsubscribe(after);
            keepRunning.set(false);
        }

        @Override
        public CompletableFuture<Boolean> drain(Duration timeout) throws InterruptedException {
            CompletableFuture<Boolean> b = super.drain(timeout);
            keepRunning.set(false);
            return b;
        }
    }

    @Override
    public MessageConsumer consume(MessageHandler handler, ConsumeOptions consumeOptions) throws IOException, JetStreamApiException {
        PullSubscribeOptions pso = PullSubscribeOptions.bind(stream, consumer);
        NatsJetStreamPullSubscription sub = (NatsJetStreamPullSubscription)js.subscribe(null, pso);
        ConsumeOptions co = orDefault(consumeOptions);
        return new InternalMessageConsumer(sub, co, handler);
    }
}
