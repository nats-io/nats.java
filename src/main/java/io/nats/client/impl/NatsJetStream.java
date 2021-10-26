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
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerConfiguration.CcNumeric;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.PublishAck;
import io.nats.client.support.JsonUtils;
import io.nats.client.support.Validator;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static io.nats.client.support.ApiConstants.SUBJECT;
import static io.nats.client.support.NatsJetStreamClientError.*;
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
            throw new IOException("Error Publishing: " + resp.getStatus().getCode() + " " + resp.getStatus().getMessage());
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
    NatsJetStreamSubscription createSubscription(String subject,
                                                 String queueName,
                                                 final NatsDispatcher dispatcher,
                                                 final MessageHandler userHandler,
                                                 final boolean autoAck,
                                                 PushSubscribeOptions pushSubscribeOptions,
                                                 PullSubscribeOptions pullSubscribeOptions
    ) throws IOException, JetStreamApiException {

        // 1. Prepare for all the validation
        boolean isPullMode = pullSubscribeOptions != null;

        SubscribeOptions so;
        String stream;
        String qgroup;
        ConsumerConfiguration userCC;

        if (isPullMode) {
            so = pullSubscribeOptions; // options must have already been checked to be non null
            stream = pullSubscribeOptions.getStream();

            userCC = so.getConsumerConfiguration();

            qgroup = null; // just to make compiler happy both paths set variable
            validateNotSupplied(userCC.getDeliverGroup(), JsSubPullCantHaveDeliverGroup);
            validateNotSupplied(userCC.getDeliverSubject(), JsSubPullCantHaveDeliverSubject);
        }
        else {
            so = pushSubscribeOptions == null ? PushSubscribeOptions.builder().build() : pushSubscribeOptions;
            stream = so.getStream(); // might be null, that's ok (see directBind)

            userCC = so.getConsumerConfiguration();

            validateNotSupplied(userCC.getMaxPullWaiting(), 0, JsSubPushCantHaveMaxPullWaiting);

            // figure out the queue name
            qgroup = validateMustMatchIfBothSupplied(userCC.getDeliverGroup(), queueName, JsSubQueueDeliverGroupMismatch);

            if (qgroup != null && userCC.getDeliverGroup() == null) {
                // the queueName was provided versus the config deliver group, so the user config must be set
                userCC = ConsumerConfiguration.builder(userCC).deliverGroup(qgroup).build();
            }
        }

        // 2A. Flow Control / heartbeat not always valid
        if (userCC.isFlowControl() || userCC.getIdleHeartbeat().toMillis() > 0) {
            if (isPullMode) {
                throw JsSubFcHbNotValidPull.instance();
            }
            if (qgroup != null) {
                throw JsSubFcHbHbNotValidQueue.instance();
            }
        }

        // 2B. Did they tell me what stream? No? look it up.
        if (stream == null) {
            stream = lookupStreamBySubject(subject);
            if (stream == null) {
                throw JsSubNoMatchingStreamForSubject.instance();
            }
        }

        ConsumerConfiguration serverCc = null;
        String consumerName = userCC.getDurable();
        String inboxDeliver = userCC.getDeliverSubject();

        // 3. Does this consumer already exist?
        if (consumerName != null) {
            ConsumerInfo serverInfo = lookupConsumerInfo(stream, consumerName);

            if (serverInfo != null) { // the consumer for that durable already exists
                serverCc = serverInfo.getConsumerConfiguration();

                if (isPullMode) {
                    if (!nullOrEmpty(serverCc.getDeliverSubject())) {
                        throw JsSubConsumerAlreadyConfiguredAsPush.instance();
                    }
                }
                else if (nullOrEmpty(serverCc.getDeliverSubject())) {
                    throw JsSubConsumerAlreadyConfiguredAsPull.instance();
                }
                else if (inboxDeliver != null && !inboxDeliver.equals(serverCc.getDeliverSubject())) {
                    throw JsSubExistingDeliverSubjectMismatch.instance();
                }

                // durable already exists, make sure the filter subject matches
                String userFilterSubject = userCC.getFilterSubject();
                if (userFilterSubject != null && !userFilterSubject.equals(serverCc.getFilterSubject())) {
                    throw JsSubSubjectDoesNotMatchFilter.instance();
                }

                if (serverCc.getDeliverGroup() == null) {
                    // lookedUp was null, means existing consumer is not a queue consumer
                    if (qgroup == null) {
                        // ok fine, no queue requested and the existing consumer is also not a queue consumer
                        // we must check if the consumer is in use though
                        if (serverInfo.isPushBound()) {
                            throw JsSubConsumerAlreadyBound.instance();
                        }
                    }
                    else { // else they requested a queue but this durable was not configured as queue
                        throw JsSubExistingConsumerNotQueue.instance();
                    }
                }
                else if (qgroup == null) {
                    throw JsSubExistingConsumerIsQueue.instance();
                }
                else if (!serverCc.getDeliverGroup().equals(qgroup)) {
                    throw JsSubExistingQueueDoesNotMatchRequestedQueue.instance();
                }

                // check to see if the user sent a different version than the server has
                // modifications are not allowed
                // previous checks for deliver subject and filter subject matching are now
                // in the changes function
                if (userIsModifiedVsServer(userCC, serverCc)) {
                    throw JsSubExistingConsumerCannotBeModified.instance();
                }

                inboxDeliver = serverCc.getDeliverSubject(); // use the deliver subject as the inbox. It may be null, that's ok, we'll fix that later
            }
            else if (so.isBind()) {
                throw JsSubConsumerNotFoundRequiredInBind.instance();
            }
        }

        // 4. If no deliver subject (inbox) provided or found, make an inbox.
        if (inboxDeliver == null) {
            inboxDeliver = conn.createInbox();
        }

        // 5. If consumer does not exist, create
        if (serverCc == null) {
            ConsumerConfiguration.Builder ccBuilder = ConsumerConfiguration.builder(userCC);

            // Pull mode doesn't maintain a deliver subject. It's actually an error if we send it.
            if (!isPullMode) {
                ccBuilder.deliverSubject(inboxDeliver);
            }

            String userFilterSubject = userCC.getFilterSubject();
            ccBuilder.filterSubject(userFilterSubject == null ? subject : userFilterSubject);

            // createOrUpdateConsumer can fail for security reasons, maybe other reasons?
            ConsumerInfo ci = createConsumerInternal(stream, ccBuilder.build());
            consumerName = ci.getName();
            serverCc = ci.getConsumerConfiguration();
        }

        // 6. create the subscription
        final String fnlStream = stream;
        final String fnlConsumerName = consumerName;
        final String fnlInboxDeliver = inboxDeliver;

        final AutoStatusManager asm = isPullMode
            ? new PullAutoStatusManager()
            : new PushAutoStatusManager(conn, so, serverCc, qgroup != null, dispatcher == null);

        NatsSubscriptionFactory factory = (sid, lSubject, lQgroup, lConn, lDispatcher)
            -> NatsJetStreamSubscription.getInstance(sid, lSubject, lQgroup, lConn, lDispatcher,
                    asm, this, isPullMode, fnlStream, fnlConsumerName, fnlInboxDeliver);

        NatsJetStreamSubscription sub;
        if (dispatcher == null) {
            sub = (NatsJetStreamSubscription) conn.createSubscription(inboxDeliver, qgroup, null, factory);
        }
        else {
            MessageHandler handler;
            if (autoAck && serverCc.getAckPolicy() != AckPolicy.None) {
                handler = msg -> {
                    if (asm.manage(msg)) { return; }  // manager handled the message
                    userHandler.onMessage(msg);
                    if (msg.lastAck() == null || msg.lastAck() == AckType.AckProgress) {
                        msg.ack();
                    }
                };
            }
            else {
                handler = msg -> {
                    if (asm.manage(msg)) { return; }  // manager handled the message
                    userHandler.onMessage(msg);
                };
            }
            sub = (NatsJetStreamSubscription) dispatcher.subscribeImplJetStream(inboxDeliver, qgroup, handler, factory);
        }

        asm.setSub(sub);
        return sub;
    }

    static boolean userIsModifiedVsServer(ConsumerConfiguration user, ConsumerConfiguration server) {

        return user.isFlowControl() != server.isFlowControl()
            || user.getDeliverPolicy() != server.getDeliverPolicy()
            || user.getAckPolicy() != server.getAckPolicy()
            || user.getReplayPolicy() != server.getReplayPolicy()

            || CcNumeric.START_SEQ.notEqual(user.getStartSequence(), server.getStartSequence())
            || CcNumeric.MAX_DELIVER.notEqual(user.getMaxDeliver(), server.getMaxDeliver())
            || CcNumeric.RATE_LIMIT.notEqual(user.getRateLimit(), server.getRateLimit())
            || CcNumeric.MAX_ACK_PENDING.notEqual(user.getMaxAckPending(), server.getMaxAckPending())
            || CcNumeric.MAX_PULL_WAITING.notEqual(user.getMaxPullWaiting(), server.getMaxPullWaiting())

            || !Objects.equals(user.getStartTime(), server.getStartTime())
            || !Objects.equals(user.getAckWait(), server.getAckWait())
            || !Objects.equals(user.getIdleHeartbeat(), server.getIdleHeartbeat())
            || !Objects.equals(user.getDescription(), server.getDescription())
            || !Objects.equals(user.getSampleFrequency(), server.getSampleFrequency());
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
        return snr.getStrings().size() == 1 ? snr.getStrings().get(0) : null;
    }
}
