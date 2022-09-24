// Copyright 2021 The NATS Authors
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
import io.nats.client.api.PublishAck;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static io.nats.client.SimpleConsumerOptions.DEFAULT_SCO_OPTIONS;
import static io.nats.client.support.Validator.validateNotNull;

public class NatsJetStream2 extends NatsJetStreamImpl implements JetStream2 {

    public NatsJetStream2(NatsConnection connection, JetStreamOptions jsOptions) throws IOException {
        super(connection, jsOptions);
    }

    // ----------------------------------------------------------------------------------------------------
    // Publish
    // ----------------------------------------------------------------------------------------------------
    /**
     * {@inheritDoc}
     */
    @Override
    public PublishAck publish(String subject, byte[] body) throws IOException, JetStreamApiException {
        return publishSyncInternal(subject, null, body, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PublishAck publish(String subject, byte[] body, PublishOptions options) throws IOException, JetStreamApiException {
        return publishSyncInternal(subject, null, body, options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PublishAck publish(Message message) throws IOException, JetStreamApiException {
        validateNotNull(message, "Message");
        return publishSyncInternal(message.getSubject(), message.getHeaders(), message.getData(), null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PublishAck publish(Message message, PublishOptions options) throws IOException, JetStreamApiException {
        validateNotNull(message, "Message");
        return publishSyncInternal(message.getSubject(), message.getHeaders(), message.getData(), options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<PublishAck> publishAsync(String subject, byte[] body) {
        return publishAsyncInternal(subject, null, body, null, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<PublishAck> publishAsync(String subject, byte[] body, PublishOptions options) {
        return publishAsyncInternal(subject, null, body, options, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<PublishAck> publishAsync(Message message) {
        validateNotNull(message, "Message");
        return publishAsyncInternal(message.getSubject(), message.getHeaders(), message.getData(), null, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<PublishAck> publishAsync(Message message, PublishOptions options) {
        validateNotNull(message, "Message");
        return publishAsyncInternal(message.getSubject(), message.getHeaders(), message.getData(), options, null);
    }

    // ----------------------------------------------------------------------------------------------------
    // Subscribe
    // ----------------------------------------------------------------------------------------------------
    @Override
    public JetStreamReader read(String stream, String consumerName) throws IOException, JetStreamApiException {
        return read(stream, consumerName, DEFAULT_SCO_OPTIONS);
    }

    @Override
    public JetStreamReader read(String stream, String consumerName, SimpleConsumerOptions sco) throws IOException, JetStreamApiException {
        validateNotNull(stream, "Stream");
        validateNotNull(consumerName, "Consumer Name");
        ConsumerConfiguration cc = _getConsumerInfo(stream, consumerName).getConsumerConfiguration();
        PullSubscribeOptions pullOpts = PullSubscribeOptions.builder()
            .stream(stream).configuration(cc).build();
        NatsJetStreamPullSubscription sub = (NatsJetStreamPullSubscription)createSubscription(
            cc.getFilterSubject(), null, null, null, false, true, true, pullOpts, null, null);
        return new NatsJetStreamPullReader(sub, sco);
    }

    @Override
    public SimpleConsumer listen(String stream, String consumerName, MessageHandler handler) throws IOException, JetStreamApiException {
        return listen(stream, consumerName, handler, DEFAULT_SCO_OPTIONS);
    }

    @Override
    public SimpleConsumer listen(String stream, String consumerName, MessageHandler handler, SimpleConsumerOptions sco) throws IOException, JetStreamApiException {
        validateNotNull(stream, "Stream");
        validateNotNull(consumerName, "Consumer Name");
        ConsumerConfiguration cc = _getConsumerInfo(stream, consumerName).getConsumerConfiguration();
        PullSubscribeOptions pullOpts = PullSubscribeOptions.builder()
            .stream(stream)
            .configuration(cc)
            .build();
        NatsDispatcher dispatcher = (NatsDispatcher)conn.createDispatcher();
        NatsJetStreamPullSubscription sub = (NatsJetStreamPullSubscription)createSubscription(
            cc.getFilterSubject(), null, dispatcher, handler, false, true, true, pullOpts, null, null);
        return new NatsSimpleConsumer(sub, sco);
    }
}
