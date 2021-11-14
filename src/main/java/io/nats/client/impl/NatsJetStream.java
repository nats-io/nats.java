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
import io.nats.client.api.*;
import io.nats.client.support.JsonUtils;
import io.nats.client.support.Validator;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
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
    private static final PushSubscribeOptions DEFAULT_PUSH_OPTS = PushSubscribeOptions.builder().build();

    JetStreamSubscription createSubscription(String subject,
                                                 String queueName,
                                                 NatsDispatcher dispatcher,
                                                 MessageHandler userHandler,
                                                 boolean isAutoAck,
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
            so = pushSubscribeOptions == null ? DEFAULT_PUSH_OPTS : pushSubscribeOptions;
            stream = so.getStream(); // might be null, that's ok (see directBind)

            userCC = so.getConsumerConfiguration();

            validateNotSupplied(userCC.getMaxPullWaiting(), ConsumerConfiguration.CcNumeric.MAX_PULL_WAITING.initial(), JsSubPushCantHaveMaxPullWaiting);

            // figure out the queue name
            qgroup = validateMustMatchIfBothSupplied(userCC.getDeliverGroup(), queueName, JsSubQueueDeliverGroupMismatch);
            if (so.isOrdered() && qgroup != null) {
                throw JsSubOrderedNotAllowOnQueues.instance();
            }
        }

        // 2A. Flow Control / heartbeat not always valid
        if (userCC.isFlowControl() || (userCC.getIdleHeartbeat() != null && userCC.getIdleHeartbeat().toMillis() > 0)) {
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

        ConsumerConfiguration serverCC = null;
        String consumerName = userCC.getDurable();
        String inboxDeliver = userCC.getDeliverSubject();

        // 3. Does this consumer already exist?
        if (consumerName != null) {
            ConsumerInfo serverInfo = lookupConsumerInfo(stream, consumerName);

            if (serverInfo != null) { // the consumer for that durable already exists
                serverCC = serverInfo.getConsumerConfiguration();

                // check to see if the user sent a different version than the server has
                // because modifications are not allowed during create subscription
                if (userCC.wouldBeChangeTo(serverCC)) {
                    throw JsSubExistingConsumerCannotBeModified.instance();
                }

                // deliver subject must be null/empty for pull, defined for push
                if (isPullMode) {
                    if (!nullOrEmpty(serverCC.getDeliverSubject())) {
                        throw JsSubConsumerAlreadyConfiguredAsPush.instance();
                    }
                }
                else if (nullOrEmpty(serverCC.getDeliverSubject())) {
                    throw JsSubConsumerAlreadyConfiguredAsPull.instance();
                }

                if (serverCC.getDeliverGroup() == null) {
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
                else if (!serverCC.getDeliverGroup().equals(qgroup)) {
                    throw JsSubExistingQueueDoesNotMatchRequestedQueue.instance();
                }

                // durable already exists, make sure the filter subject matches
                if (subject == null) { // allowed if they had given both stream and durable
                    subject = userCC.getFilterSubject();
                }
                else if (!isFilterMatch(subject, serverCC.getFilterSubject(), stream)) {
                    throw JsSubSubjectDoesNotMatchFilter.instance();
                }

                inboxDeliver = serverCC.getDeliverSubject(); // use the deliver subject as the inbox. It may be null, that's ok, we'll fix that later
            }
            else if (so.isBind()) {
                throw JsSubConsumerNotFoundRequiredInBind.instance();
            }
        }

        return finishCreateSubscription(subject, dispatcher, userHandler, isAutoAck, isPullMode, so, stream, qgroup, userCC, serverCC, consumerName, inboxDeliver, null);
    }

    // this was separated expressly so ordered consumer can skip the beginning
    JetStreamSubscription finishCreateSubscription(final String subject,
                                                   final NatsDispatcher dispatcher,
                                                   final MessageHandler userHandler,
                                                   final boolean isAutoAck,
                                                   final boolean isPullMode,
                                                   final SubscribeOptions so,
                                                   final String stream,
                                                   final String qgroup,
                                                   final ConsumerConfiguration userCC,
                                                   final ConsumerConfiguration serverCC,
                                                   final String consumerName,
                                                   final String inboxDeliver,
                                                   NatsJetStreamOrderedSubscription ordered
    ) throws IOException, JetStreamApiException {

        // 4. If no deliver subject (inbox) provided or found, make an inbox.
        final String fnlInboxDeliver = inboxDeliver == null ? conn.createInbox() : inboxDeliver;

        // 5. If consumer does not exist, create
        final String fnlConsumerName;
        final ConsumerConfiguration fnlServerCC;
        if (serverCC == null) {
            ConsumerConfiguration.Builder ccBuilder = ConsumerConfiguration.builder(userCC);

            // Pull mode doesn't maintain a deliver subject. It's actually an error if we send it.
            if (!isPullMode) {
                ccBuilder.deliverSubject(fnlInboxDeliver);
            }

            if (userCC.getFilterSubject() == null) {
                ccBuilder.filterSubject(subject);
            }

            ccBuilder.deliverGroup(qgroup);

            // createOrUpdateConsumer can fail for security reasons, maybe other reasons?
            ConsumerInfo ci = _createConsumer(stream, ccBuilder.build());
            fnlConsumerName = ci.getName();
            fnlServerCC = ci.getConsumerConfiguration();
        }
        else {
            fnlConsumerName = consumerName;
            fnlServerCC = serverCC;
        }

        // 6. create the subscription. lambda needs final or effectively final vars
        final StatusManager statusManager = isPullMode
            ? new PullStatusManager()
            : new PushStatusManager(conn, so, fnlServerCC, qgroup != null, dispatcher == null);

        boolean existingOrdered = ordered != null;

        if (ordered == null && so.isOrdered()) {
            ordered = new NatsJetStreamOrderedSubscription(this, subject, dispatcher, userHandler, isAutoAck, so, stream, fnlServerCC);
        }

        NatsSubscriptionFactory factory = (sid, lSubject, lQgroup, lConn, lDispatcher)
            -> NatsJetStreamSubscription.getInstance(sid, lSubject, lQgroup, lConn, lDispatcher,
            statusManager, this, isPullMode, stream, fnlConsumerName, fnlInboxDeliver);

        NatsJetStreamSubscription sub;
        if (dispatcher == null) {
            sub = (NatsJetStreamSubscription) conn.createSubscription(fnlInboxDeliver, qgroup, null, factory);
        }
        else {
            MessageManager orderedManager = ordered == null ? m -> false : ordered.manager();
            MessageManager autoAckManager = !isAutoAck || fnlServerCC.getAckPolicy() == AckPolicy.None
                ? m -> false
                : m -> {
                    if (m.lastAck() == null || m.lastAck() == AckType.AckProgress) {
                        m.ack();
                    }
                    return false;
                };

            MessageHandler handler = msg -> {
                if (statusManager.manage(msg)) { return; }  // manager handled the message
                if (orderedManager.manage(msg)) { return; }  // manager handled the message
                userHandler.onMessage(msg);
                autoAckManager.manage(msg);
            };
            sub = (NatsJetStreamSubscription) dispatcher.subscribeImplJetStream(fnlInboxDeliver, qgroup, handler, factory);
        }

        statusManager.setSub(sub);

        if (ordered != null) {
            ordered.setCurrent(sub);
            if (!existingOrdered) {
                return ordered;
            }
        }

        return sub;
    }

    private boolean isFilterMatch(String subscribeSubject, String filterSubject, String stream) throws IOException, JetStreamApiException {

        // subscribeSubject guaranteed to not be null
        // filterSubject may be null or empty or have value

        if (subscribeSubject.equals(filterSubject)) {
            return true;
        }

        if (nullOrEmpty(filterSubject) || filterSubject.equals(">")) {
            // lookup stream subject returns null if there is not exactly one subject
            String streamSubject = lookupStreamSubject(stream);
            return subscribeSubject.equals(streamSubject);
        }

        return false;
    }

    private String lookupStreamSubject(String stream) throws IOException, JetStreamApiException {
        StreamInfo si = _getStreamInfo(stream);
        List<String> streamSubjects = si.getConfiguration().getSubjects();
        return streamSubjects.size() == 1 ? streamSubjects.get(0) : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject) throws IOException, JetStreamApiException {
        validateSubject(subject, true);
        return createSubscription(subject, null, null, null, false, null, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, PushSubscribeOptions options) throws IOException, JetStreamApiException {
        validateSubject(subject, isSubjectRequired(options));
        return createSubscription(subject, null, null, null, false, options, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, String queue, PushSubscribeOptions options) throws IOException, JetStreamApiException {
        validateSubject(subject, isSubjectRequired(options));
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
        validateSubject(subject, isSubjectRequired(options));
        validateNotNull(dispatcher, "Dispatcher");
        validateNotNull(handler, "Handler");
        return createSubscription(subject, null, (NatsDispatcher) dispatcher, handler, autoAck, options, null);
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
        return createSubscription(subject, queue, (NatsDispatcher) dispatcher, handler, autoAck, options, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, PullSubscribeOptions options) throws IOException, JetStreamApiException {
        validateNotNull(options, "Pull Subscribe Options");
        validateSubject(subject, isSubjectRequired(options));
        return createSubscription(subject, null, null, null, false, null, options);
    }

    private boolean isSubjectRequired(SubscribeOptions options) {
        return options == null || !options.isBind();
    }

    // ----------------------------------------------------------------------------------------------------
    // General Utils
    // ----------------------------------------------------------------------------------------------------
    ConsumerInfo lookupConsumerInfo(String stream, String consumer) throws IOException, JetStreamApiException {
        try {
            return _getConsumerInfo(stream, consumer);
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
