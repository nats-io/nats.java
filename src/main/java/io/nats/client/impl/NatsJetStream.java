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
    NatsJetStreamSubscription createSubscription(String subject, String queueName,
                                                 NatsDispatcher dispatcher, MessageHandler userMh, boolean autoAck,
                                                 PushSubscribeOptions pushSubscribeOptions,
                                                 PullSubscribeOptions pullSubscribeOptions) throws IOException, JetStreamApiException {

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
            validateNotSupplied(userCC.getDeliverGroup(), "[SUB-PL01] Pull Mode can't have a deliver group.");
            validateNotSupplied(userCC.getDeliverSubject(), "[SUB-PL02] Pull Mode can't have a deliver subject.");
        }
        else {
            so = pushSubscribeOptions == null ? PushSubscribeOptions.builder().build() : pushSubscribeOptions;
            stream = so.getStream(); // might be null, that's ok (see directBind)

            userCC = so.getConsumerConfiguration();

            validateNotSupplied(userCC.getMaxPullWaiting(), 0, "[SUB-PS01] Push mode cannot supply max pull waiting.");

            // figure out the queue name
            qgroup = validateMustMatchIfBothSupplied(userCC.getDeliverGroup(), queueName, "[SUB-QM01] Consumer Configuration DeliverGroup", "Queue Name");

            if (qgroup != null && !qgroup.equals(userCC.getDeliverGroup())) {
                // the queueName was provided versus the config deliver group, so the user config must be set
                userCC = ConsumerConfiguration.builder(userCC).deliverGroup(qgroup).build();
            }
        }

        // 2A. Flow Control / heartbeat not always valid
        if (userCC.isFlowControl() || userCC.getIdleHeartbeat().toMillis() > 0) {
            if (isPullMode) {
                throw new IllegalArgumentException("[SUB-FH01] Flow Control and/or Heartbeat is not valid with a Pull subscription.");
            }
            else if (qgroup != null) {
                throw new IllegalArgumentException("[SUB-FH02] Flow Control and/or Heartbeat is not valid in Queue Mode.");
            }
        }

        // 2B. Did they tell me what stream? No? look it up.
        if (stream == null) {
            stream = lookupStreamBySubject(subject);
            if (stream == null) {
                throw new IllegalStateException("[SUB-ST01] No matching streams for subject: " + subject);
            }
        }

        ConsumerConfiguration targetCC = null;
        String consumerName = userCC.getDurable();
        String inboxDeliver = userCC.getDeliverSubject();

        // 3. Does this consumer already exist?
        if (consumerName != null) {
            ConsumerInfo lookedUpInfo = lookupConsumerInfo(stream, consumerName);

            if (lookedUpInfo != null) { // the consumer for that durable already exists
                targetCC = lookedUpInfo.getConsumerConfiguration();

                String lookedUp = targetCC.getDeliverSubject();
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
                lookedUp = targetCC.getFilterSubject();
                String userFilterSubject = userCC.getFilterSubject();
                if (userFilterSubject != null && !userFilterSubject.equals(lookedUp)) {
                    throw new IllegalArgumentException(
                            String.format("[SUB-FS01] Subject '%s' mismatches consumer configuration '%s'.", subject, userFilterSubject));
                }

                lookedUp = targetCC.getDeliverGroup();
                if (lookedUp == null) {
                    // lookedUp was null, means existing consumer is not a queue consumer
                    if (qgroup == null) {
                        // ok fine, no queue requested and the existing consumer is also not a queue consumer
                        // we must check if the consumer is in use though
                        if (lookedUpInfo.isPushBound()) {
                            throw new IllegalArgumentException(String.format("[SUB-PB01] Consumer '%s' is already bound to a subscription.", consumerName));
                        }
                    }
                    else { // else they requested a queue but this durable was not configured as queue
                        throw new IllegalArgumentException(String.format("[SUB-QU01] Existing consumer '%s' is not configured as a queue / deliver group.", consumerName));
                    }
                }
                else if (qgroup == null) {
                    throw new IllegalArgumentException(String.format("[SUB-QU02] Existing consumer '%s' is configured as a queue / deliver group.", consumerName));
                }
                else if (!lookedUp.equals(qgroup)) {
                    throw new IllegalArgumentException(
                        String.format("[SUB-QU03] Existing consumer deliver group '%s' does not match requested queue / deliver group '%s'.", lookedUp, qgroup));
                }

                // check to see if the user sent a different version than the server has
                // modifications are not allowed
                // previous checks for deliver subject and filter subject matching are now
                // in the changes function
                String changes = userVersusServer(userCC, targetCC);
                if (changes != null) {
                    throw new IllegalArgumentException("[SUB-CC01] Existing consumer cannot be modified. " + changes);
                }

                inboxDeliver = targetCC.getDeliverSubject(); // use the deliver subject as the inbox. It may be null, that's ok, we'll fix that later
            }
            else if (so.isBind()) {
                throw new IllegalArgumentException("[SUB-BM01] Consumer not found for durable. Required in bind mode.");
            }
        }

        // 4. If no deliver subject (inbox) provided or found, make an inbox.
        if (inboxDeliver == null) {
            inboxDeliver = conn.createInbox();
        }

        // 5. If consumer does not exist, create
        if (targetCC == null) {
            ConsumerConfiguration.Builder ccBuilder = ConsumerConfiguration.builder(userCC);

            // Pull mode doesn't maintain a deliver subject. It's actually an error if we send it.
            if (!isPullMode) {
                ccBuilder.deliverSubject(inboxDeliver);
            }

            // being discussed if this is correct, but leave it for now.
            String userFilterSubject = userCC.getFilterSubject();
            ccBuilder.filterSubject(userFilterSubject == null ? subject : userFilterSubject);

            // createOrUpdateConsumer can fail for security reasons, maybe other reasons?
            ConsumerInfo ci = createConsumerInternal(stream, ccBuilder.build());
            consumerName = ci.getName();
            targetCC = ci.getConsumerConfiguration();
        }

        // 6. create the subscription
        final String fnlStream = stream;
        final String fnlConsumerName = consumerName;
        final String fnlInboxDeliver = inboxDeliver;

        NatsJetStreamAutoStatusManager asm =
            new NatsJetStreamAutoStatusManager(conn, so, targetCC, qgroup != null, dispatcher == null);

        NatsSubscriptionFactory factory = (sid, lSubject, lQgroup, lConn, lDispatcher)
            -> NatsJetStreamSubscription.getInstance(sid, lSubject, lQgroup, lConn, lDispatcher,
                    asm, this, isPullMode, fnlStream, fnlConsumerName, fnlInboxDeliver);

        NatsJetStreamSubscription sub;
        if (dispatcher == null) {
            sub = (NatsJetStreamSubscription) conn.createSubscription(inboxDeliver, qgroup, null, factory);
        }
        else {
            NatsJetStreamSubscriptionMessageHandler njssmh =
                new NatsJetStreamSubscriptionMessageHandler(asm, userMh, autoAck);
            sub = (NatsJetStreamSubscription) dispatcher.subscribeImplJetStream(inboxDeliver, qgroup, njssmh, factory);
        }

        asm.setSub(sub);
        return sub;
    }

    static String userVersusServer(ConsumerConfiguration user, ConsumerConfiguration server) {

        StringBuilder sb = new StringBuilder();
        comp(sb, user.isFlowControl(), server.isFlowControl(), "Flow Control");
        comp(sb, user.getDeliverPolicy(), server.getDeliverPolicy(), "Deliver Policy");
        comp(sb, user.getAckPolicy(), server.getAckPolicy(), "Ack Policy");
        comp(sb, user.getReplayPolicy(), server.getReplayPolicy(), "Replay Policy");

        comp(sb, user.getStartSequence(), server.getStartSequence(), CcNumeric.START_SEQ);
        comp(sb, user.getMaxDeliver(), server.getMaxDeliver(), CcNumeric.MAX_DELIVER);
        comp(sb, user.getRateLimit(), server.getRateLimit(), CcNumeric.RATE_LIMIT);
        comp(sb, user.getMaxAckPending(), server.getMaxAckPending(), CcNumeric.MAX_ACK_PENDING);
        comp(sb, user.getMaxPullWaiting(), server.getMaxPullWaiting(), CcNumeric.MAX_PULL_WAITING);

        comp(sb, user.getDescription(), server.getDescription(), "Description");
        comp(sb, user.getStartTime(), server.getStartTime(), "Start Time");
        comp(sb, user.getAckWait(), server.getAckWait(), "Ack Wait");
        comp(sb, user.getSampleFrequency(), server.getSampleFrequency(), "Sample Frequency");
        comp(sb, user.getIdleHeartbeat(), server.getIdleHeartbeat(), "Idle Heartbeat");

        return sb.length() == 0 ? null : sb.toString();
    }

    private static void comp(StringBuilder sb, Object requested, Object retrieved, String name) {
        if (!Objects.equals(requested, retrieved)) {
            appendErr(sb, requested, retrieved, name);
        }
    }

    private static void comp(StringBuilder sb, long requested, long retrieved, CcNumeric field) {
        if (field.comparable(requested) != field.comparable(retrieved)) {
            appendErr(sb, requested, retrieved, field.getErr());
        }
    }

    private static void appendErr(StringBuilder sb, Object requested, Object retrieved, String name) {
        if (sb.length() > 0) {
            sb.append(", ");
        }
        sb.append(name).append(" [").append(requested).append(" vs. ").append(retrieved).append(']');
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
