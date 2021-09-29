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
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.PublishAck;
import io.nats.client.support.JsonUtils;
import io.nats.client.support.Validator;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static io.nats.client.support.ApiConstants.SUBJECT;
import static io.nats.client.support.Validator.*;

public class NatsJetStream extends NatsJetStreamImplBase implements JetStream {

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
        return publishSyncInternal(subject, null, body, false, null);
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
        return publishSyncInternal(message.getSubject(), message.getHeaders(), message.getData(), message.isUtf8mode(), null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PublishAck publish(Message message, PublishOptions options) throws IOException, JetStreamApiException {
        validateNotNull(message, "Message");
        return publishSyncInternal(message.getSubject(), message.getHeaders(), message.getData(), message.isUtf8mode(), options);
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
        return publishAsyncInternal(message.getSubject(), message.getHeaders(), message.getData(), message.isUtf8mode(), null, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<PublishAck> publishAsync(Message message, PublishOptions options) {
        validateNotNull(message, "Message");
        return publishAsyncInternal(message.getSubject(), message.getHeaders(), message.getData(), message.isUtf8mode(), options, null);
    }

    private PublishAck publishSyncInternal(String subject, Headers headers, byte[] data, PublishOptions options) throws IOException, JetStreamApiException {
        return publishSyncInternal(subject, headers, data, false, options);
    }

    @Deprecated // Plans are to remove allowing utf8mode
    private PublishAck publishSyncInternal(String subject, Headers headers, byte[] data, boolean utf8mode, PublishOptions options) throws IOException, JetStreamApiException {
        Headers merged = mergePublishOptions(headers, options);

        if (jso.isPublishNoAck()) {
            conn.publishInternal(subject, null, merged, data, utf8mode);
            return null;
        }

        Duration timeout = options == null ? jso.getRequestTimeout() : options.getStreamTimeout();

        Message resp = makeInternalRequestResponseRequired(subject, merged, data, utf8mode, timeout, false);
        return processPublishResponse(resp, options);
    }

    private CompletableFuture<PublishAck> publishAsyncInternal(String subject, Headers headers, byte[] data, PublishOptions options, Duration knownTimeout) {
        return publishAsyncInternal(subject, headers, data, false, options, knownTimeout);
    }

    @Deprecated // Plans are to remove allowing utf8mode
    private CompletableFuture<PublishAck> publishAsyncInternal(String subject, Headers headers, byte[] data, boolean utf8mode, PublishOptions options, Duration knownTimeout) {
        Headers merged = mergePublishOptions(headers, options);

        if (jso.isPublishNoAck()) {
            conn.publishInternal(subject, null, merged, data, utf8mode);
            return null;
        }

        CompletableFuture<Message> future = conn.requestFutureInternal(subject, merged, data, utf8mode, knownTimeout, false);

        return future.thenCompose(resp -> {
            try {
                responseRequired(resp);
                return CompletableFuture.completedFuture(processPublishResponse(resp, options));
            } catch (IOException | JetStreamApiException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private PublishAck processPublishResponse(Message resp, PublishOptions options) throws IOException, JetStreamApiException {
        if (resp.isStatusMessage()) {
            if (resp.getStatus().getCode() == 503) {
                throw new IOException("Error Publishing: No stream available.");
            }
            else {
                throw new IOException("Error Publishing: " + resp.getStatus().getMessage());
            }
        }

        PublishAck ack = new PublishAck(resp);
        String ackStream = ack.getStream();
        String pubStream = options == null ? null : options.getStream();
        // stream specified in options but different than ack should not happen but...
        if (pubStream != null && !pubStream.equals(ackStream)) {
            throw new IOException("Expected ack from stream " + pubStream + ", received from: " + ackStream);
        }
        return ack;
    }

    private Headers mergePublishOptions(Headers headers, PublishOptions opts) {
        // never touch the user's original headers
        Headers merged = headers == null ? null : new Headers(headers);

        if (opts != null) {
            merged = mergeNum(merged, EXPECTED_LAST_SEQ_HDR, opts.getExpectedLastSequence());
            merged = mergeNum(merged, EXPECTED_LAST_SUB_SEQ_HDR, opts.getExpectedLastSubjectSequence());
            merged = mergeString(merged, EXPECTED_LAST_MSG_ID_HDR, opts.getExpectedLastMsgId());
            merged = mergeString(merged, EXPECTED_STREAM_HDR, opts.getExpectedStream());
            merged = mergeString(merged, MSG_ID_HDR, opts.getMessageId());
        }

        return merged;
    }

    private Headers mergeNum(Headers h, String key, long value) {
        return value > -1 ? _mergeNum(h, key, Long.toString(value)): h;
    }

    private Headers mergeString(Headers h, String key, String value) {
        return Validator.nullOrEmpty(value) ? h : _mergeNum(h, key, value);
    }

    private Headers _mergeNum(Headers h, String key, String value) {
        if (h == null) {
            h = new Headers();
        }
        return h.add(key, value);
    }

    // ----------------------------------------------------------------------------------------------------
    // Subscribe
    // ----------------------------------------------------------------------------------------------------
    NatsJetStreamSubscription createSubscription(String subject, String queueName,
                                                 NatsDispatcher dispatcher, MessageHandler userMh, boolean autoAck,
                                                 PushSubscribeOptions pushSubscribeOptions,
                                                 PullSubscribeOptions pullSubscribeOptions) throws IOException, JetStreamApiException {
        // first things first...
        boolean isPullMode = pullSubscribeOptions != null;

        // setup the configuration, use a default.
        String stream;
        ConsumerConfiguration.Builder ccBuilder;
        SubscribeOptions so;

        if (isPullMode) {
            so = pullSubscribeOptions; // options must have already been checked to be non null
            stream = pullSubscribeOptions.getStream();
            ccBuilder = ConsumerConfiguration.builder(pullSubscribeOptions.getConsumerConfiguration());
            ccBuilder.deliverSubject(null); // pull mode can't have a deliver subject
            queueName = null; // should already be, just make sure
            ccBuilder.deliverGroup(null);   // pull mode can't have a deliver group
        }
        else {
            so = pushSubscribeOptions == null ? PushSubscribeOptions.builder().build() : pushSubscribeOptions;
            stream = so.getStream(); // might be null, that's ok (see directBind)
            ccBuilder = ConsumerConfiguration.builder(so.getConsumerConfiguration());
            ccBuilder.maxPullWaiting(0); // this does not apply to push, in fact will error b/c deliver subject will be set
            // deliver subject does not have to be cleared
            // figure out the queue name
            queueName = validateMustMatchIfBothSupplied(ccBuilder.getDeliverGroup(), queueName,
                    "[SUB-Q01] Consumer Configuration DeliverGroup", "Queue Name");
            ccBuilder.deliverGroup(queueName); // and set it in case the deliver group was null
        }

        boolean bindMode = so.isBind();

        ConsumerConfiguration consumerConfig = null;
        String consumerName = ccBuilder.getDurable();
        String inboxDeliver = ccBuilder.getDeliverSubject();
        String filterSubject = ccBuilder.getFilterSubject();

        // 1. Did they tell me what stream? No? look it up.
        // subscribe options will have already validated that stream is present for direct mode
        if (stream == null) {
            stream = lookupStreamBySubject(subject);
        }

        // 2. Is this a durable or ephemeral
        if (consumerName != null) {
            ConsumerInfo lookedUpInfo = lookupConsumerInfo(stream, consumerName);

            if (lookedUpInfo != null) { // the consumer for that durable already exists
                consumerConfig = lookedUpInfo.getConsumerConfiguration();

                String lookedUp = consumerConfig.getDeliverSubject();
                if (isPullMode) {
                    if (!nullOrEmpty(lookedUp)) {
                        throw new IllegalArgumentException(
                                String.format("[SUB-DS01] Consumer is already configured as a push consumer with deliver subject '%s'.", lookedUp));
                    }
                }
                else if (nullOrEmpty(lookedUp)) {
                    throw new IllegalArgumentException("[SUB-DS02] Consumer is already configured as a pull consumer with no deliver subject.");
                }
                else if (inboxDeliver != null && !inboxDeliver.equals(lookedUp)) {
                    throw new IllegalArgumentException(
                            String.format("[SUB-DS03] Existing consumer deliver subject '%s' does not match requested deliver subject '%s'.", lookedUp, inboxDeliver));
                }

                // durable already exists, make sure the filter subject matches
                lookedUp = consumerConfig.getFilterSubject();
                if (filterSubject != null && !filterSubject.equals(lookedUp)) {
                    throw new IllegalArgumentException(
                            String.format("[SUB-FS01] Subject '%s' mismatches consumer configuration '%s'.", subject, filterSubject));
                }
                filterSubject = lookedUp;

                lookedUp = consumerConfig.getDeliverGroup();
                if (lookedUp == null) {
                    // lookedUp was null, means existing consumer is not a queue consumer
                    if (queueName == null) {
                        // ok fine, no queue requested and the existing consumer is also not a queue consumer
                        // we must check if the consumer is in use though
                        if (lookedUpInfo.isPushBound()) {
                            throw new IllegalArgumentException(String.format("[SUB-PB01] Consumer '%s' is already bound to a subscription.", consumerName));
                        }
                    }
                    else { // else they requested a queue but this durable was not configured as queue
                        throw new IllegalArgumentException(String.format("[SUB-Q01] Existing consumer '%s' is not configured as a queue / deliver group.", consumerName));
                    }
                }
                else if (queueName == null) {
                    throw new IllegalArgumentException(String.format("[SUB-Q02] Existing consumer '%s' is configured as a queue / deliver group.", consumerName));
                }
                else if (!lookedUp.equals(queueName)) {
                    throw new IllegalArgumentException(
                            String.format("[SUB-Q03] Existing consumer deliver group '%s' does not match requested queue / deliver group '%s'.", lookedUp, queueName));
                }

                inboxDeliver = consumerConfig.getDeliverSubject(); // use the deliver subject as the inbox. It may be null, that's ok, we'll fix that later
            }
            else if (bindMode) {
                throw new IllegalArgumentException("[SUB-BND01] Consumer not found for durable. Required in bind mode.");
            }
        }

        // 3. If no deliver subject (inbox) provided or found, make an inbox.
        if (inboxDeliver == null) {
            inboxDeliver = conn.createInbox();
        }

        // 4. If consumer does not exist, create
        if (consumerConfig == null) {
            // Pull mode doesn't maintain a deliver subject. It's actually an error if we send it.
            if (!isPullMode) {
                ccBuilder.deliverSubject(inboxDeliver);
            }

            // being discussed if this is correct, but leave it for now.
            ccBuilder.filterSubject(filterSubject == null ? subject : filterSubject);

            // createOrUpdateConsumer can fail for security reasons, maybe other reasons?
            ConsumerInfo ci = createConsumerInternal(stream, ccBuilder.build());
            consumerName = ci.getName();
            consumerConfig = ci.getConsumerConfiguration();
        }

        // 5. Queue Mode Check
        boolean queueMode = queueName != null;
        if (queueMode && (consumerConfig.getFlowControl() || consumerConfig.getIdleHeartbeat().toMillis() > 0)) {
            throw new IllegalArgumentException("[SUB-QM01] Cannot use queue when consumer has Flow Control or Heartbeat.");
        }

        // 6. create the subscription
        final String fnlStream = stream;
        final String fnlConsumerName = consumerName;
        final String fnlInboxDeliver = inboxDeliver;
        final ConsumerConfiguration fnlConsumerConfig = consumerConfig;
        NatsSubscriptionFactory nsf = (sid, lSubject, lQueueName, lConn, lDispatcher)
            -> new NatsJetStreamSubscription(sid, lSubject, lQueueName, lConn, lDispatcher,
            NatsJetStream.this, isPullMode, so, fnlStream, fnlConsumerName, fnlInboxDeliver, fnlConsumerConfig);

        NatsJetStreamSubscription sub;
        if (dispatcher == null) {
            sub = (NatsJetStreamSubscription) conn.createSubscription(inboxDeliver, queueName, null, nsf);
        }
        else {
            NatsJetStreamSubscriptionMessageHandler njssmh = new NatsJetStreamSubscriptionMessageHandler(conn, userMh, autoAck, queueMode, so, consumerConfig);
            // if not necessary, just give the sub the user's handler
            if (njssmh.isNecessary()) {
                sub = (NatsJetStreamSubscription) dispatcher.subscribeImplJetStream(inboxDeliver, queueName, njssmh, nsf);
                // chicken or egg situation here. The handler needs the sub in case of error,
                // but the sub needs the handler in order to be created
                njssmh.setSub(sub);
            }
            else {
                sub = (NatsJetStreamSubscription) dispatcher.subscribeImplJetStream(inboxDeliver, queueName, userMh, nsf);
            }
        }

        return sub;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject) throws IOException, JetStreamApiException {
        validateSubject(subject, true);
        return createSubscription(subject, null, null, null, false, null, null);
    }

    @Override
    public JetStreamSubscription subscribe(String subject, PushSubscribeOptions options) throws IOException, JetStreamApiException {
        validateSubject(subject, true);
        return createSubscription(subject, null, null, null, false, options, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, String queue, PushSubscribeOptions options) throws IOException, JetStreamApiException {
        validateSubject(subject, true);
        queue = emptyAsNull(validateQueueName(queue, false));
        return createSubscription(subject, queue, null, null, false, options, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, Dispatcher dispatcher, MessageHandler handler, boolean autoAck) throws IOException, JetStreamApiException {
        validateSubject(subject, true);
        validateNotNull(dispatcher, "Dispatcher");
        validateNotNull(handler, "Handler");
        return createSubscription(subject, null, (NatsDispatcher) dispatcher, handler, autoAck, null, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, Dispatcher dispatcher, MessageHandler handler, boolean autoAck, PushSubscribeOptions options) throws IOException, JetStreamApiException {
        validateSubject(subject, true);
        validateNotNull(dispatcher, "Dispatcher");
        validateNotNull(handler, "Handler");
        return createSubscription(subject, null, (NatsDispatcher) dispatcher, handler, autoAck, options, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, String queue, Dispatcher dispatcher, MessageHandler handler, boolean autoAck, PushSubscribeOptions options) throws IOException, JetStreamApiException {
        validateSubject(subject, true);
        queue = emptyAsNull(validateQueueName(queue, false));
        validateNotNull(dispatcher, "Dispatcher");
        validateNotNull(handler, "Handler");
        return createSubscription(subject, queue, (NatsDispatcher) dispatcher, handler, autoAck, options, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, PullSubscribeOptions options) throws IOException, JetStreamApiException {
        validateSubject(subject, true);
        validateNotNull(options, "Options");
        return createSubscription(subject, null, null, null, false, null, options);
    }

    // ----------------------------------------------------------------------------------------------------
    // General Utils
    // ----------------------------------------------------------------------------------------------------
    ConsumerInfo lookupConsumerInfo(String stream, String consumer) throws IOException, JetStreamApiException {
        try {
            return getConsumerInfo(stream, consumer);
        }
        catch (JetStreamApiException e) {
            // the right side of this condition...  ( starting here \/ ) is for backward compatibility with server versions that did not provide api error codes
            if (e.getApiErrorCode() == JS_CONSUMER_NOT_FOUND_ERR || (e.getErrorCode() == 404 && e.getErrorDescription().contains("consumer"))) {
                return null;
            }
            throw e;
        }
    }

    protected String lookupStreamBySubject(String subject) throws IOException, JetStreamApiException {
        byte[] body = JsonUtils.simpleMessageBody(SUBJECT, subject);
        StreamNamesReader snr = new StreamNamesReader();
        Message resp = makeRequestResponseRequired(JSAPI_STREAM_NAMES, body, jso.getRequestTimeout());
        snr.process(resp);
        if (snr.getStrings().size() != 1) {
            throw new IllegalStateException("No matching streams for subject: " + subject);
        }
        return snr.getStrings().get(0);
    }
}
