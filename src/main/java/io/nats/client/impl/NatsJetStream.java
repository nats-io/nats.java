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
import java.util.ArrayList;
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

    static class SidCheckManager extends MessageManager {
        @Override
        boolean manage(Message msg) {
            return !sub.getSID().equals(msg.getSID());
        }
    }

    interface PushStatusMessageManagerFactory {
        PushStatusMessageManager createPushStatusMessageManager(
            NatsConnection conn, SubscribeOptions so, ConsumerConfiguration cc, boolean queueMode, boolean syncMode);
    }

    static PushStatusMessageManagerFactory PUSH_STATUS_MANAGER_FACTORY = PushStatusMessageManager::new;

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

            if (userCC.maxPullWaitingWasSet()) {
                throw JsSubPushCantHaveMaxPullWaiting.instance();
            }

            if (userCC.maxBatchWasSet()) {
                throw JsSubPushCantHaveMaxBatch.instance();
            }

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
        final String fnlStream;
        if (stream == null) {
            fnlStream = lookupStreamBySubject(subject);
            if (fnlStream == null) {
                throw JsSubNoMatchingStreamForSubject.instance();
            }
        }
        else {
            fnlStream = stream;
        }

        ConsumerConfiguration serverCC = null;
        String consumerName = userCC.getDurable();
        String inboxDeliver = userCC.getDeliverSubject();

        // 3. Does this consumer already exist?
        if (consumerName != null) {
            ConsumerInfo serverInfo = lookupConsumerInfo(fnlStream, consumerName);

            if (serverInfo != null) { // the consumer for that durable already exists
                serverCC = serverInfo.getConsumerConfiguration();

                // check to see if the user sent a different version than the server has
                // because modifications are not allowed during create subscription
                ConsumerConfigurationComparer userCCC = new ConsumerConfigurationComparer(userCC);
                List<String> changes = userCCC.getChanges(serverCC);
                if (changes.size() > 0) {
                    throw JsSubExistingConsumerCannotBeModified.instance("Changed fields: " + changes.toString());
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
                if (nullOrEmpty(subject)) { // allowed if they had given both stream and durable
                    subject = userCC.getFilterSubject();
                }
                else if (!isFilterMatch(subject, serverCC.getFilterSubject(), fnlStream)) {
                    throw JsSubSubjectDoesNotMatchFilter.instance();
                }

                inboxDeliver = serverCC.getDeliverSubject(); // use the deliver subject as the inbox. It may be null, that's ok, we'll fix that later
            }
            else if (so.isBind()) {
                throw JsSubConsumerNotFoundRequiredInBind.instance();
            }
        }

        // 4. If no deliver subject (inbox) provided or found, make an inbox.
        final String fnlInboxDeliver = inboxDeliver == null ? conn.createInbox() : inboxDeliver;

        // 5. If consumer does not exist, create and settle on the config. Name will have to wait
        //    If the consumer exists, I know what the settled info is
        final String settledConsumerName;
        final ConsumerConfiguration settledServerCC;
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

            settledServerCC = ccBuilder.build();
            settledConsumerName = null;
        }
        else {
            settledServerCC = serverCC;
            settledConsumerName = consumerName;
        }

        // 6. create the subscription. lambda needs final or effectively final vars
        NatsJetStreamSubscription sub;
        if (isPullMode) {
            final MessageManager[] managers = new MessageManager[] { new PullStatusMessageManager() };
            final NatsSubscriptionFactory factory = (sid, lSubject, lQgroup, lConn, lDispatcher)
                -> new NatsJetStreamPullSubscription(sid, lSubject, lConn, this, fnlStream, settledConsumerName, managers);
            sub = (NatsJetStreamSubscription) conn.createSubscription(fnlInboxDeliver, qgroup, null, factory);
        }
        else {
            final MessageManager statusManager =
                PUSH_STATUS_MANAGER_FACTORY.createPushStatusMessageManager(conn, so, settledServerCC, qgroup != null, dispatcher == null);
            final MessageManager[] managers;
            if (so.isOrdered()) {
                managers = new MessageManager[3];
                managers[0] = new SidCheckManager();
                managers[1] = statusManager;
                managers[2] = new OrderedManager(this, dispatcher, fnlStream, settledServerCC);
            }
            else {
                managers = new MessageManager[1];
                managers[0] = statusManager;
            }

            final NatsSubscriptionFactory factory = (sid, lSubject, lQgroup, lConn, lDispatcher)
                -> new NatsJetStreamSubscription(sid, lSubject, lQgroup, lConn, lDispatcher,
                this, fnlStream, settledConsumerName, managers);

            if (dispatcher == null) {
                sub = (NatsJetStreamSubscription) conn.createSubscription(fnlInboxDeliver, qgroup, null, factory);
            }
            else {
                AsyncMessageHandler handler = new AsyncMessageHandler(userHandler, isAutoAck, settledServerCC, managers);
                sub = (NatsJetStreamSubscription) dispatcher.subscribeImplJetStream(fnlInboxDeliver, qgroup, handler, factory);
            }
        }

        // 7. The consumer might need to be created, do it here
        if (settledConsumerName == null) {
            try {
                ConsumerInfo ci = _createConsumer(fnlStream, settledServerCC);
                sub.setConsumerName(ci.getName());
            }
            catch (IOException | JetStreamApiException e) {
                // create consumer can fail, unsubscribe and then throw the exception to the user
                if (dispatcher == null) {
                    sub.unsubscribe();
                }
                else {
                    dispatcher.unsubscribe(sub);
                }
                throw e;
            }
        }

        return sub;
    }

    static class ConsumerConfigurationComparer extends ConsumerConfiguration {
        public ConsumerConfigurationComparer(ConsumerConfiguration cc) {
            super(cc);
        }

        public List<String> getChanges(ConsumerConfiguration serverCc) {
            ConsumerConfigurationComparer serverCcc = new ConsumerConfigurationComparer(serverCc);
            List<String> changes = new ArrayList<>();

            record(deliverPolicy != null && deliverPolicy != serverCcc.getDeliverPolicy(), "deliverPolicy", changes);
            record(ackPolicy != null && ackPolicy != serverCcc.getAckPolicy(), "ackPolicy", changes);
            record(replayPolicy != null && replayPolicy != serverCcc.getReplayPolicy(), "replayPolicy", changes);

            record(flowControl != null && flowControl != serverCcc.isFlowControl(), "flowControl", changes);
            record(headersOnly != null && headersOnly != serverCcc.isHeadersOnly(), "headersOnly", changes);

            record(startSeq != null && !startSeq.equals(serverCcc.getStartSequence()), "startSequence", changes);
            record(rateLimit != null && !rateLimit.equals(serverCcc.getStartSequence()), "rateLimit", changes);

            // MaxDeliver is a special case because -1 and 0 are unset where other unsigned -1 is unset
            record(LongChangeHelper.MAX_DELIVER.wouldBeChange(maxDeliver, serverCcc.maxDeliver), "maxDeliver", changes);

            record(maxAckPending != null && !maxAckPending.equals(serverCcc.getMaxAckPending()), "maxAckPending", changes);
            record(maxPullWaiting != null && !maxPullWaiting.equals(serverCcc.getMaxPullWaiting()), "maxPullWaiting", changes);
            record(maxBatch != null && !maxBatch.equals(serverCcc.getMaxBatch()), "maxBatch", changes);

            record(ackWait != null && !ackWait.equals(getOrUnset(serverCcc.ackWait)), "ackWait", changes);
            record(idleHeartbeat != null && !idleHeartbeat.equals(getOrUnset(serverCcc.idleHeartbeat)), "idleHeartbeat", changes);
            record(maxExpires != null && !maxExpires.equals(getOrUnset(serverCcc.maxExpires)), "maxExpires", changes);
            record(inactiveThreshold != null && !inactiveThreshold.equals(getOrUnset(serverCcc.inactiveThreshold)), "inactiveThreshold", changes);

            record(startTime != null && !startTime.equals(serverCcc.startTime), "startTime", changes);

            record(filterSubject != null && !filterSubject.equals(serverCcc.filterSubject), "filterSubject", changes);
            record(description != null && !description.equals(serverCcc.description), "description", changes);
            record(sampleFrequency != null && !sampleFrequency.equals(serverCcc.sampleFrequency), "sampleFrequency", changes);
            record(deliverSubject != null && !deliverSubject.equals(serverCcc.deliverSubject), "deliverSubject", changes);
            record(deliverGroup != null && !deliverGroup.equals(serverCcc.deliverGroup), "deliverGroup", changes);

            record(!backoff.equals(serverCcc.backoff), "backoff", changes); // backoff will never be null, but can be empty

            // do not need to check Durable because the original is retrieved by the durable name

            return changes;
        }

        private void record(boolean isChange, String field, List<String> changes) {
            if (isChange) { changes.add(field); }
        }
    }

    static class AsyncMessageHandler implements MessageHandler {
        List<MessageManager> managers;
        List<MessageHandler> handlers;

        public AsyncMessageHandler(MessageHandler userHandler, boolean isAutoAck, ConsumerConfiguration settledServerCC, MessageManager ... managers) {
            handlers = new ArrayList<>();
            handlers.add(userHandler);
            if (isAutoAck && settledServerCC.getAckPolicy() != AckPolicy.None) {
                handlers.add(Message::ack);
            };

            this.managers = new ArrayList<>();
            for (MessageManager mm : managers) {
                if (mm != null) {
                    this.managers.add(mm);
                }
            }
        }

        @Override
        public void onMessage(Message msg) throws InterruptedException {
            for (MessageManager mm : managers) {
                if (mm.manage(msg)) {
                    return;
                }
            }

            for (MessageHandler mh : handlers) {
                mh.onMessage(msg);
            }
        }
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
        StreamInfo si = _getStreamInfo(stream, null);
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
