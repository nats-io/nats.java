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
import io.nats.client.api.PublishAck;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static io.nats.client.support.Validator.*;

public class NatsJetStream extends NatsJetStreamImpl implements JetStream {

    public NatsJetStream(NatsConnection connection, JetStreamOptions jsOptions) throws IOException {
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

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject) throws IOException, JetStreamApiException {
        validateSubject(subject, true);
        return createSubscription(subject, null, null, null, false, false, false, null, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, PushSubscribeOptions options) throws IOException, JetStreamApiException {
        validateSubject(subject, isSubjectRequired(options));
        return createSubscription(subject, null, null, null, false, false, false, options, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, String queue, PushSubscribeOptions options) throws IOException, JetStreamApiException {
        validateSubject(subject, isSubjectRequired(options));
        queue = emptyAsNull(validateQueueName(queue, false));
        return createSubscription(subject, queue, null, null, false, false, false, options, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, Dispatcher dispatcher, MessageHandler handler, boolean autoAck) throws IOException, JetStreamApiException {
        validateSubject(subject, true);
        validateNotNull(dispatcher, "Dispatcher");
        validateNotNull(handler, "Handler");
        return createSubscription(subject, null, (NatsDispatcher) dispatcher, handler, autoAck, false, false, null, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, Dispatcher dispatcher, MessageHandler handler, boolean autoAck, PushSubscribeOptions options) throws IOException, JetStreamApiException {
        validateSubject(subject, isSubjectRequired(options));
        validateNotNull(dispatcher, "Dispatcher");
        validateNotNull(handler, "Handler");
        return createSubscription(subject, null, (NatsDispatcher) dispatcher, handler, autoAck, false, false, options, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, String queue, Dispatcher dispatcher, MessageHandler handler, boolean autoAck, PushSubscribeOptions options) throws IOException, JetStreamApiException {
        validateSubject(subject, isSubjectRequired(options));
        queue = emptyAsNull(validateQueueName(queue, false));
        validateNotNull(dispatcher, "Dispatcher");
        validateNotNull(handler, "Handler");
        return createSubscription(subject, queue, (NatsDispatcher) dispatcher, handler, autoAck, false, false, options, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, PullSubscribeOptions options) throws IOException, JetStreamApiException {
        validateNotNull(options, "Pull Subscribe Options");
        validateSubject(subject, isSubjectRequired(options));
        return createSubscription(subject, null, null, null, false, true, false, options, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, Dispatcher dispatcher, MessageHandler handler, PullSubscribeOptions options) throws IOException, JetStreamApiException {
        validateNotNull(options, "Pull Subscribe Options");
        validateSubject(subject, isSubjectRequired(options));
        return createSubscription(subject, null, (NatsDispatcher) dispatcher, handler, false, true, false, options, null);
    }

    private boolean isSubjectRequired(SubscribeOptions options) {
        return options == null || !options.isBind();
    }
}
