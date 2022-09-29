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
import io.nats.client.support.NatsJetStreamConstants;
import io.nats.client.support.Validator;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static io.nats.client.support.ApiConstants.SUBJECT;
import static io.nats.client.support.NatsJetStreamClientError.*;
import static io.nats.client.support.Validator.*;

class NatsJetStreamImpl implements NatsJetStreamConstants {

    // currently the only thing we care about caching is the allowDirect setting
    static class CachedStreamInfo {
        public final boolean allowDirect;

        public CachedStreamInfo(StreamInfo si) {
            allowDirect = si.getConfiguration().getAllowDirect();
        }
    }

    private static final ConcurrentHashMap<String, CachedStreamInfo> CACHED_STREAM_INFO_MAP = new ConcurrentHashMap<>();

    final NatsConnection conn;
    final JetStreamOptions jso;
    final boolean consumerCreate290Available;

    // ----------------------------------------------------------------------------------------------------
    // Create / Init
    // ----------------------------------------------------------------------------------------------------
    NatsJetStreamImpl(NatsConnection connection, JetStreamOptions jsOptions) throws IOException {
        conn = connection;
        jso = JetStreamOptions.builder(jsOptions).build(); // builder handles null
        consumerCreate290Available = conn.getInfo().isSameOrNewerThanVersion("2.9.0") && !jso.isOptOut290ConsumerCreate();
    }

    // ----------------------------------------------------------------------------------------------------
    // Management that is also needed by regular context
    // ----------------------------------------------------------------------------------------------------
    ConsumerInfo _getConsumerInfo(String streamName, String consumer) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_CONSUMER_INFO, streamName, consumer);
        Message resp = makeRequestResponseRequired(subj, null, jso.getRequestTimeout());
        return new ConsumerInfo(resp).throwOnHasError();
    }

    ConsumerInfo _createConsumer(String streamName, ConsumerConfiguration config) throws IOException, JetStreamApiException {
        String name = config.getName();
        if (name != null && !consumerCreate290Available) {
            throw JsConsumerCreate290NotAvailable.instance();
        }

        String durable = config.getDurable();

        String consumerName = name == null ? durable : name;

        String subj;
        if (consumerName == null) { // just use old template
            subj = String.format(JSAPI_CONSUMER_CREATE, streamName);
        }
        else if (consumerCreate290Available) {
            String fs = config.getFilterSubject();
            if (fs == null || fs.equals(">")) {
                subj = String.format(JSAPI_CONSUMER_CREATE_V290, streamName, consumerName);
            }
            else {
                subj = String.format(JSAPI_CONSUMER_CREATE_V290_W_FILTER, streamName, consumerName, fs);
            }
        }
        else { // server is old and consumerName must be durable since name was checked for JsConsumerCreate290NotAvailable
            subj = String.format(JSAPI_DURABLE_CREATE, streamName, durable);
        }

        ConsumerCreateRequest ccr = new ConsumerCreateRequest(streamName, config);
        Message resp = makeRequestResponseRequired(subj, ccr.serialize(), conn.getOptions().getConnectionTimeout());
        return new ConsumerInfo(resp).throwOnHasError();
    }

    void _createConsumerUnsubscribeOnException(String stream, ConsumerConfiguration cc, NatsJetStreamSubscription sub) throws IOException, JetStreamApiException {
        try {
            ConsumerInfo ci = _createConsumer(stream, cc);
            sub.setConsumerName(ci.getName());
        }
        catch (IOException | JetStreamApiException e) {
            // create consumer can fail, unsubscribe and then throw the exception to the user
            if (sub.getDispatcher() == null) {
                sub.unsubscribe();
            }
            else {
                sub.getDispatcher().unsubscribe(sub);
            }
            throw e;
        }
    }

    StreamInfo _getStreamInfo(String streamName, StreamInfoOptions options) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_STREAM_INFO, streamName);
        byte[] payload = options == null ? null : options.serialize();
        Message resp = makeRequestResponseRequired(subj, payload, jso.getRequestTimeout());
        return createAndCacheStreamInfoThrowOnError(streamName, resp);
    }

    StreamInfo createAndCacheStreamInfoThrowOnError(String streamName, Message resp) throws JetStreamApiException {
        return cacheStreamInfo(streamName, new StreamInfo(resp).throwOnHasError());
    }

    StreamInfo cacheStreamInfo(String streamName, StreamInfo si) {
        CACHED_STREAM_INFO_MAP.put(streamName, new CachedStreamInfo(si));
        return si;
    }

    List<StreamInfo> cacheStreamInfo(List<StreamInfo> list) {
        list.forEach(si -> CACHED_STREAM_INFO_MAP.put(si.getConfiguration().getName(), new CachedStreamInfo(si)));
        return list;
    }

    List<String> _getStreamNamesBySubjectFilter(String subjectFilter) throws IOException, JetStreamApiException {
        byte[] body = JsonUtils.simpleMessageBody(SUBJECT, subjectFilter);
        StreamNamesReader snr = new StreamNamesReader();
        Message resp = makeRequestResponseRequired(JSAPI_STREAM_NAMES, body, jso.getRequestTimeout());
        snr.process(resp);
        return snr.getStrings();
    }

    // ----------------------------------------------------------------------------------------------------
    // Request Utils
    // ----------------------------------------------------------------------------------------------------
    Message makeRequestResponseRequired(String subject, byte[] bytes, Duration timeout) throws IOException {
        try {
            return responseRequired(conn.request(prependPrefix(subject), bytes, timeout));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    Message makeInternalRequestResponseRequired(String subject, Headers headers, byte[] data, boolean utf8mode, Duration timeout, boolean cancelOn503) throws IOException {
        try {
            return responseRequired(conn.requestInternal(subject, headers, data, utf8mode, timeout, cancelOn503));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    Message responseRequired(Message respMessage) throws IOException {
        if (respMessage == null) {
            throw new IOException("Timeout or no response waiting for NATS JetStream server");
        }
        return respMessage;
    }

    String prependPrefix(String subject) {
        return jso.getPrefix() + subject;
    }

    CachedStreamInfo getCachedStreamInfo(String streamName) throws IOException, JetStreamApiException {
        CachedStreamInfo csi = CACHED_STREAM_INFO_MAP.get(streamName);
        if (csi != null) {
            return csi;
        }
        _getStreamInfo(streamName, null);
        return CACHED_STREAM_INFO_MAP.get(streamName);
    }

    // ----------------------------------------------------------------------------------------------------
    // Publish
    // ----------------------------------------------------------------------------------------------------
    PublishAck publishSyncInternal(String subject, Headers headers, byte[] data, PublishOptions options) throws IOException, JetStreamApiException {
        Headers merged = mergePublishOptions(headers, options);

        if (jso.isPublishNoAck()) {
            conn.publishInternal(subject, null, merged, data, true);
            return null;
        }

        Duration timeout = options == null ? jso.getRequestTimeout() : options.getStreamTimeout();

        Message resp = makeInternalRequestResponseRequired(subject, merged, data, true, timeout, false);
        return processPublishResponse(resp, options);
    }

    CompletableFuture<PublishAck> publishAsyncInternal(String subject, Headers headers, byte[] data, PublishOptions options, Duration knownTimeout) {
        Headers merged = mergePublishOptions(headers, options);

        if (jso.isPublishNoAck()) {
            conn.publishInternal(subject, null, merged, data, true);
            return null;
        }

        CompletableFuture<Message> future = conn.requestFutureInternal(subject, merged, data, true, knownTimeout, false);

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

    interface MessageManagerFactory {
        MessageManager createMessageManager(
            NatsConnection conn, SubscribeOptions so, ConsumerConfiguration cc, boolean queueMode, boolean syncMode);
    }

    MessageManagerFactory DEFAULT_PUSH_MESSAGE_MANAGER_FACTORY = PushMessageManager::new;
    MessageManagerFactory DEFAULT_PULL_MESSAGE_MANAGER_FACTORY =
        (mmConn, mmSo, mmCo, mmQueueMode, mmSyncMode) -> new PullMessageManager(mmConn, mmSyncMode);

    JetStreamSubscription createSubscription(String subject,
                                             String queueName,
                                             NatsDispatcher dispatcher,
                                             MessageHandler userHandler,
                                             boolean isAutoAck,
                                             boolean isPullMode,
                                             boolean isSimplificationMode,
                                             SubscribeOptions subscribeOptions,
                                             MessageManagerFactory messageManagerFactory
    ) throws IOException, JetStreamApiException {

        // 1. Prepare for all the validation
        SubscribeOptions so;
        String stream;
        String qgroup;
        ConsumerConfiguration userCC;

        if (isPullMode) {
            so = subscribeOptions; // options must have already been checked to be non-null
            stream = subscribeOptions.getStream();

            userCC = so.getConsumerConfiguration();

            qgroup = null; // just to make compiler happy both paths set variable
            validateNotSupplied(userCC.getDeliverGroup(), JsSubPullCantHaveDeliverGroup);
            validateNotSupplied(userCC.getDeliverSubject(), JsSubPullCantHaveDeliverSubject);
        }
        else {
            so = subscribeOptions == null ? DEFAULT_PUSH_OPTS : subscribeOptions;
            stream = so.getStream(); // might be null, that's ok (see directBind)

            userCC = so.getConsumerConfiguration();

            if (userCC.maxPullWaitingWasSet()) { throw JsSubPushCantHaveMaxPullWaiting.instance(); }
            if (userCC.maxBatchWasSet())       { throw JsSubPushCantHaveMaxBatch.instance(); }
            if (userCC.maxBytesWasSet())       { throw JsSubPushCantHaveMaxBytes.instance(); }

            // figure out the queue name
            qgroup = validateMustMatchIfBothSupplied(userCC.getDeliverGroup(), queueName, JsSubQueueDeliverGroupMismatch);
            if (so.isOrdered() && qgroup != null) {
                throw JsSubOrderedNotAllowOnQueues.instance();
            }
        }

        // 2A. Flow Control / heartbeat not always valid
        if (userCC.getIdleHeartbeat() != null && userCC.getIdleHeartbeat().toMillis() > 0) {
            if (isPullMode) {
                throw JsSubFcHbNotValidPull.instance();
            }
            if (qgroup != null) {
                throw JsSubFcHbNotValidQueue.instance();
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
        String durableName = userCC.getDurable();
        String inboxDeliver = userCC.getDeliverSubject();

        // 3. Does this consumer already exist?
        if (durableName != null) {
            if (isSimplificationMode) {
                serverCC = userCC;
                inboxDeliver = serverCC.getDeliverSubject(); // use the deliver subject as the inbox. It may be null, that's ok, we'll fix that later
            }
            else {
                ConsumerInfo serverInfo = lookupConsumerInfo(fnlStream, durableName);
                if (serverInfo != null) { // the consumer for that durable already exists
                    serverCC = serverInfo.getConsumerConfiguration();

                    // check to see if the user sent a different version than the server has
                    // because modifications are not allowed during create subscription
                    ConsumerConfigurationComparer userCCC = new ConsumerConfigurationComparer(userCC);
                    List<String> changes = userCCC.getChanges(serverCC);
                    if (changes.size() > 0) {
                        throw JsSubExistingConsumerCannotBeModified.instance("Changed fields: " + changes);
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
            settledConsumerName = durableName; // will be null if not durable
        }

        // 6. create the subscription. lambda needs final or effectively final vars
        NatsJetStreamSubscription sub;
        final MessageManager[] managers;
        final NatsSubscriptionFactory factory;
        if (isPullMode) {
            if (messageManagerFactory == null) {
                messageManagerFactory = DEFAULT_PULL_MESSAGE_MANAGER_FACTORY;
            }
            managers = new MessageManager[] { messageManagerFactory.createMessageManager(conn, so, settledServerCC, false, dispatcher == null) };
            factory = (sid, lSubject, lQgroup, lConn, lDispatcher) ->
                new NatsJetStreamPullSubscription(sid, lSubject, lConn, lDispatcher,
                    this, fnlStream, settledConsumerName, managers);
        }
        else {
            if (messageManagerFactory == null) {
                messageManagerFactory = DEFAULT_PUSH_MESSAGE_MANAGER_FACTORY;
            }
            final MessageManager pushMessageManager =
                messageManagerFactory.createMessageManager(conn, so, settledServerCC, qgroup != null, dispatcher == null);
            if (so.isOrdered()) {
                managers = new MessageManager[3];
                managers[0] = new SidCheckManager();
                managers[1] = pushMessageManager;
                managers[2] = new OrderedManager(this, dispatcher, fnlStream, settledServerCC);
            }
            else {
                managers = new MessageManager[] { pushMessageManager };
            }
            factory = (sid, lSubject, lQgroup, lConn, lDispatcher) ->
                new NatsJetStreamSubscription(sid, lSubject, lQgroup, lConn, lDispatcher,
                    this, fnlStream, settledConsumerName, managers);
        }
        if (dispatcher == null) {
            sub = (NatsJetStreamSubscription) conn.createSubscription(fnlInboxDeliver, qgroup, null, factory);
        }
        else {
            AsyncMessageHandler handler = new AsyncMessageHandler(userHandler, isAutoAck, settledServerCC, managers);
            sub = (NatsJetStreamSubscription) dispatcher.subscribeImplJetStream(fnlInboxDeliver, qgroup, handler, factory);
        }

        // 7. The consumer might need to be created, do it here
        if (settledConsumerName == null) {
            _createConsumerUnsubscribeOnException(fnlStream, settledServerCC, sub);
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

            if (deliverPolicy != null && deliverPolicy != serverCcc.getDeliverPolicy()) { changes.add("deliverPolicy"); };
            if (ackPolicy != null && ackPolicy != serverCcc.getAckPolicy()) { changes.add("ackPolicy"); };
            if (replayPolicy != null && replayPolicy != serverCcc.getReplayPolicy()) { changes.add("replayPolicy"); };

            if (flowControl != null && flowControl != serverCcc.isFlowControl()) { changes.add("flowControl"); };
            if (headersOnly != null && headersOnly != serverCcc.isHeadersOnly()) { changes.add("headersOnly"); };
            if (memStorage != null && memStorage != serverCcc.isMemStorage()) { changes.add("memStorage"); };

            if (startSeq != null && !startSeq.equals(serverCcc.getStartSequence())) { changes.add("startSequence"); };
            if (rateLimit != null && !rateLimit.equals(serverCcc.getStartSequence())) { changes.add("rateLimit"); };

            if (maxDeliver != null && !maxDeliver.equals(serverCcc.getMaxDeliver())) { changes.add("maxDeliver"); };
            if (maxAckPending != null && !maxAckPending.equals(serverCcc.getMaxAckPending())) { changes.add("maxAckPending"); };
            if (maxPullWaiting != null && !maxPullWaiting.equals(serverCcc.getMaxPullWaiting())) { changes.add("maxPullWaiting"); };
            if (maxBatch != null && !maxBatch.equals(serverCcc.getMaxBatch())) { changes.add("maxBatch"); };
            if (maxBytes != null && !maxBytes.equals(serverCcc.getMaxBytes())) { changes.add("maxBytes"); };

            if (ackWait != null && !ackWait.equals(getOrUnset(serverCcc.ackWait))) { changes.add("ackWait"); };
            if (idleHeartbeat != null && !idleHeartbeat.equals(getOrUnset(serverCcc.idleHeartbeat))) { changes.add("idleHeartbeat"); };
            if (maxExpires != null && !maxExpires.equals(getOrUnset(serverCcc.maxExpires))) { changes.add("maxExpires"); };
            if (inactiveThreshold != null && !inactiveThreshold.equals(getOrUnset(serverCcc.inactiveThreshold))) { changes.add("inactiveThreshold"); };

            if (startTime != null && !startTime.equals(serverCcc.startTime)) { changes.add("startTime"); };

            if (filterSubject != null && !filterSubject.equals(serverCcc.filterSubject)) { changes.add("filterSubject"); };
            if (description != null && !description.equals(serverCcc.description)) { changes.add("description"); };
            if (sampleFrequency != null && !sampleFrequency.equals(serverCcc.sampleFrequency)) { changes.add("sampleFrequency"); };
            if (deliverSubject != null && !deliverSubject.equals(serverCcc.deliverSubject)) { changes.add("deliverSubject"); };
            if (deliverGroup != null && !deliverGroup.equals(serverCcc.deliverGroup)) { changes.add("deliverGroup"); };

            if (!backoff.equals(serverCcc.backoff)) { changes.add("backoff"); }; // backoff will never be null, but can be empty

            if (numReplicas != null && !numReplicas.equals(serverCcc.numReplicas)) { changes.add("numReplicas"); };

            // do not need to check Durable because the original is retrieved by the durable name

            return changes;
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

    String lookupStreamBySubject(String subject) throws IOException, JetStreamApiException {
        List<String> list = _getStreamNamesBySubjectFilter(subject);
        return list.size() == 1 ? list.get(0) : null;
    }
}
