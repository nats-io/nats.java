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
import io.nats.client.api.*;
import io.nats.client.support.Validator;

import java.io.IOException;
import java.util.List;

import static io.nats.client.ConsumeOptions.DEFAULT_CONSUME_OPTIONS;
import static io.nats.client.support.ConsumerUtils.generateConsumerName;

/**
 * SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
 */
class NatsStreamContext implements StreamContext {
    final String streamName;
    final NatsJetStream js;
    final NatsJetStreamManagement jsm;

    // for when this is contructed from the NatsJetStream itself
    NatsStreamContext(String streamName, NatsJetStream js, NatsConnection connection, JetStreamOptions jsOptions) throws IOException, JetStreamApiException {
        this.streamName = streamName;
        this.js = js == null ? new NatsJetStream(connection, jsOptions) : js;
        jsm = new NatsJetStreamManagement(connection, jsOptions);
        jsm.getStreamInfo(streamName); // this is just verifying that the stream exists
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getStreamName() {
        return streamName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamInfo getStreamInfo() throws IOException, JetStreamApiException {
        return jsm.getStreamInfo(streamName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamInfo getStreamInfo(StreamInfoOptions options) throws IOException, JetStreamApiException {
        return jsm.getStreamInfo(streamName, options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PurgeResponse purge() throws IOException, JetStreamApiException {
        return jsm.purgeStream(streamName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PurgeResponse purge(PurgeOptions options) throws IOException, JetStreamApiException {
        return jsm.purgeStream(streamName, options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerContext consumerContext(String consumerName) throws IOException, JetStreamApiException {
        return new NatsConsumerContext(this, jsm.getConsumerInfo(streamName, consumerName));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerContext createOrUpdateConsumer(ConsumerConfiguration config) throws IOException, JetStreamApiException {
        return new NatsConsumerContext(this, jsm.addOrUpdateConsumer(streamName, config));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean deleteConsumer(String consumerName) throws IOException, JetStreamApiException {
        return jsm.deleteConsumer(streamName, consumerName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerInfo getConsumerInfo(String consumerName) throws IOException, JetStreamApiException {
        return jsm.getConsumerInfo(streamName, consumerName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getConsumerNames() throws IOException, JetStreamApiException {
        return jsm.getConsumerNames(streamName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<ConsumerInfo> getConsumers() throws IOException, JetStreamApiException {
        return jsm.getConsumers(streamName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageInfo getMessage(long seq) throws IOException, JetStreamApiException {
        return jsm.getMessage(streamName, seq);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageInfo getLastMessage(String subject) throws IOException, JetStreamApiException {
        return jsm.getLastMessage(streamName, subject);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageInfo getFirstMessage(String subject) throws IOException, JetStreamApiException {
        return jsm.getFirstMessage(streamName, subject);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageInfo getNextMessage(long seq, String subject) throws IOException, JetStreamApiException {
        return jsm.getNextMessage(streamName, seq, subject);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean deleteMessage(long seq) throws IOException, JetStreamApiException {
        return jsm.deleteMessage(streamName, seq);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean deleteMessage(long seq, boolean erase) throws IOException, JetStreamApiException {
        return jsm.deleteMessage(streamName, seq, erase);
    }

    static class OrderedPullSubscribeOptionsBuilder extends PullSubscribeOptions.Builder {
        public OrderedPullSubscribeOptionsBuilder(String streamName, ConsumerConfiguration cc) {
            stream(streamName);
            configuration(cc);
            ordered = true;
        }
    }

    @Override
    public IterableConsumer orderedConsume(OrderedConsumerConfiguration config) throws IOException, JetStreamApiException {
        return orderedConsume(config, DEFAULT_CONSUME_OPTIONS);
    }

    @Override
    public IterableConsumer orderedConsume(OrderedConsumerConfiguration config, ConsumeOptions consumeOptions) throws IOException, JetStreamApiException {
        Validator.required(config, "Ordered Consumer Config");
        Validator.required(consumeOptions, "Consume Options");
        ConsumerConfiguration cc = getBackingConsumerConfiguration(config);
        PullSubscribeOptions pso = new OrderedPullSubscribeOptionsBuilder(streamName, cc).build();
        return new NatsIterableConsumer(new SimplifiedSubscriptionMaker(js, pso, cc.getFilterSubject()), null, consumeOptions);
    }

    @Override
    public MessageConsumer orderedConsume(OrderedConsumerConfiguration config, MessageHandler handler) throws IOException, JetStreamApiException {
        return orderedConsume(config, handler, DEFAULT_CONSUME_OPTIONS);
    }

    @Override
    public MessageConsumer orderedConsume(OrderedConsumerConfiguration config, MessageHandler handler, ConsumeOptions consumeOptions) throws IOException, JetStreamApiException {
        Validator.required(config, "Ordered Consumer Config");
        Validator.required(handler, "Message Handler");
        Validator.required(consumeOptions, "Consume Options");
        ConsumerConfiguration cc = getBackingConsumerConfiguration(config);
        PullSubscribeOptions pso = new OrderedPullSubscribeOptionsBuilder(streamName, cc).build();
        return new NatsMessageConsumer(new SimplifiedSubscriptionMaker(js, pso, cc.getFilterSubject()), null, handler, consumeOptions);
    }

    private ConsumerConfiguration getBackingConsumerConfiguration(OrderedConsumerConfiguration config) {
        return ConsumerConfiguration.builder()
            .name(generateConsumerName())
            .filterSubject(config.getFilterSubject())
            .deliverPolicy(config.getDeliverPolicy())
            .startSequence(config.getStartSequence())
            .startTime(config.getStartTime())
            .replayPolicy(config.getReplayPolicy())
            .headersOnly(config.getHeadersOnly())
            .build();
    }
}
