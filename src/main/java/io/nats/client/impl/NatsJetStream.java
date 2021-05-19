package io.nats.client.impl;

import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.client.support.NatsJetStreamConstants;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.nats.client.support.ApiConstants.SEQ;
import static io.nats.client.support.JsonUtils.simpleMessageBody;
import static io.nats.client.support.Validator.*;

public class NatsJetStream implements JetStream, JetStreamManagement, NatsJetStreamConstants {

    private final NatsConnection conn;
    private final JetStreamOptions jso;

    // ----------------------------------------------------------------------------------------------------
    // Create / Init
    // ----------------------------------------------------------------------------------------------------
    NatsJetStream(NatsConnection connection, JetStreamOptions jsOptions) throws IOException {
        conn = connection;
        jso = JetStreamOptions.builder(jsOptions).build(); // builder handles null

        checkEnabled();
    }

    private void checkEnabled() throws IOException {
        try {
            Message respMessage = makeRequest(JSAPI_ACCOUNT_INFO, null, jso.getRequestTimeout());
            if (respMessage == null) {
                throw new IllegalStateException("JetStream is not enabled.");
            }

            AccountStatistics stats = new AccountStatistics(respMessage);
            if (stats.getErrorCode() == 503) {
                throw new IllegalStateException(stats.getDescription());
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // Manage
    // ----------------------------------------------------------------------------------------------------

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamInfo addStream(StreamConfiguration config) throws IOException, JetStreamApiException {
        return addOrUpdateStream(config, JSAPI_STREAM_CREATE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamInfo updateStream(StreamConfiguration config) throws IOException, JetStreamApiException {
        return addOrUpdateStream(config, JSAPI_STREAM_UPDATE);
    }

    private StreamInfo addOrUpdateStream(StreamConfiguration config, String template) throws IOException, JetStreamApiException {
        if (config == null) {
            throw new IllegalArgumentException("Configuration cannot be null.");
        }
        String streamName = config.getName();
        if (nullOrEmpty(streamName)) {
            throw new IllegalArgumentException("Configuration must have a valid stream name");
        }

        String subj = String.format(template, streamName);
        Message resp = makeRequestResponseRequired(subj, config.toJson().getBytes(), jso.getRequestTimeout());
        return new StreamInfo(resp).throwOnHasError();
    }

    @Override
    public boolean deleteStream(String streamName) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_STREAM_DELETE, streamName);
        Message resp = makeRequestResponseRequired(subj, null, jso.getRequestTimeout());
        return new SuccessApiResponse(resp).throwOnHasError().getSuccess();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamInfo getStreamInfo(String streamName) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_STREAM_INFO, streamName);
        Message resp = makeRequestResponseRequired(subj, null, jso.getRequestTimeout());
        return new StreamInfo(resp).throwOnHasError();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PurgeResponse purgeStream(String streamName) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_STREAM_PURGE, streamName);
        Message resp = makeRequestResponseRequired(subj, null, jso.getRequestTimeout());
        return new PurgeResponse(resp).throwOnHasError();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerInfo addOrUpdateConsumer(String streamName, ConsumerConfiguration config) throws IOException, JetStreamApiException {
        validateStreamName(streamName, true);
        validateNotNull(config, "Config");
        validateNotNull(config.getDurable(), "Durable");
        return addOrUpdateConsumerInternal(streamName, config);
    }

    private ConsumerInfo addOrUpdateConsumerInternal(String streamName, ConsumerConfiguration config) throws IOException, JetStreamApiException {
        String durable = config.getDurable();
        String requestJSON = new ConsumerCreateRequest(streamName, config).toJson();

        String subj;
        if (durable == null) {
            subj = String.format(JSAPI_CONSUMER_CREATE, streamName);
        } else {
            subj = String.format(JSAPI_DURABLE_CREATE, streamName, durable);
        }
        Message resp = makeRequestResponseRequired(subj, requestJSON.getBytes(), conn.getOptions().getConnectionTimeout());
        return new ConsumerInfo(resp).throwOnHasError();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean deleteConsumer(String streamName, String consumer) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_CONSUMER_DELETE, streamName, consumer);
        Message resp = makeRequestResponseRequired(subj, null, jso.getRequestTimeout());
        return new SuccessApiResponse(resp).throwOnHasError().getSuccess();
    }

    @Override
    public ConsumerInfo getConsumerInfo(String streamName, String consumer) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_CONSUMER_INFO, streamName, consumer);
        Message resp = makeRequestResponseRequired(subj, null, jso.getRequestTimeout());
        return new ConsumerInfo(resp).throwOnHasError();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getConsumerNames(String streamName) throws IOException, JetStreamApiException {
        return getConsumerNames(streamName, null);
    }

    // TODO FUTURE resurface this api publicly when server supports
    // @Override
    private List<String> getConsumerNames(String streamName, String filter) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_CONSUMER_NAMES, streamName);
        ConsumerNamesReader cnr = new ConsumerNamesReader();
        while (cnr.hasMore()) {
            Message resp = makeRequestResponseRequired(subj, cnr.nextJson(filter), jso.getRequestTimeout());
            cnr.process(resp);
        }
        return cnr.getStrings();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<ConsumerInfo> getConsumers(String streamName) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_CONSUMER_LIST, streamName);
        ConsumerListReader clg = new ConsumerListReader();
        while (clg.hasMore()) {
            Message resp = makeRequestResponseRequired(subj, clg.nextJson(), jso.getRequestTimeout());
            clg.process(resp);
        }
        return clg.getConsumers();
    }

    @Override
    public List<String> getStreamNames() throws IOException, JetStreamApiException {
        StreamNamesReader snr = new StreamNamesReader();
        while (snr.hasMore()) {
            Message resp = makeRequestResponseRequired(JSAPI_STREAMS, snr.nextJson(), jso.getRequestTimeout());
            snr.process(resp);
        }
        return snr.getStrings();
    }

    @Override
    public List<StreamInfo> getStreams() throws IOException, JetStreamApiException {
        StreamListReader slg = new StreamListReader();
        while (slg.hasMore()) {
            Message resp = makeRequestResponseRequired(JSAPI_STREAM_LIST, slg.nextJson(), jso.getRequestTimeout());
            slg.process(resp);
        }
        return slg.getStreams();
    }

    @Override
    public MessageInfo getMessage(String streamName, long seq) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_MSG_GET, streamName);
        Message resp = makeRequestResponseRequired(subj, simpleMessageBody(SEQ, seq), jso.getRequestTimeout());
        return new MessageInfo(resp).throwOnHasError();
    }

    @Override
    public boolean deleteMessage(String streamName, long seq) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_MSG_DELETE, streamName);
        Message resp = makeRequestResponseRequired(subj, simpleMessageBody(SEQ, seq), jso.getRequestTimeout());
        return new SuccessApiResponse(resp).throwOnHasError().getSuccess();
    }

    // ----------------------------------------------------------------------------------------------------
    // Publish
    // ---------------------------------------------------------------------------------------------NatsJsPullSub-------

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

        Message resp = makeRequestResponseRequired(subject, merged, data, utf8mode, timeout, false);
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
        if (isStreamSpecified(pubStream) && !pubStream.equals(ackStream)) {
            throw new IOException("Expected ack from stream " + pubStream + ", received from: " + ackStream);
        }
        return ack;
    }

    private Headers mergePublishOptions(Headers headers, PublishOptions options) {
        Headers piHeaders;

        if (options == null) {
            piHeaders = headers == null ? null : new Headers(headers);
        }
        else {
            piHeaders = new Headers(headers);

            // we know no headers are set with default options
            long seqno = options.getExpectedLastSequence();
            if (seqno > 0) {
                piHeaders.add(EXPECTED_LAST_SEQ_HDR, Long.toString(seqno));
            }

            String s = options.getExpectedLastMsgId();
            if (s != null) {
                piHeaders.add(EXPECTED_LAST_MSG_ID_HDR, s);
            }

            s = options.getExpectedStream();
            if (s != null) {
                piHeaders.add(EXPECTED_STREAM_HDR, s);
            }

            s = options.getMessageId();
            if (s != null) {
                piHeaders.add(MSG_ID_HDR, s);
            }
        }
        return piHeaders;
    }

    private boolean isStreamSpecified(String streamName) {
        return streamName != null;
    }

    // ----------------------------------------------------------------------------------------------------
    // Subscribe
    // ----------------------------------------------------------------------------------------------------
    NatsJetStreamSubscription createSubscription(String subject, String queueName,
                                                 NatsDispatcher dispatcher, MessageHandler handler, boolean autoAck,
                                                 PushSubscribeOptions pushSubscribeOptions,
                                                 PullSubscribeOptions pullSubscribeOptions) throws IOException, JetStreamApiException {
        // first things first...
        boolean isPullMode = pullSubscribeOptions != null;

        // setup the configuration, use a default.
        String stream;
        ConsumerConfiguration.Builder ccBuilder;
        SubscribeOptions so;

        if (isPullMode) {
            so = pullSubscribeOptions;
            stream = pullSubscribeOptions.getStream();
            ccBuilder = ConsumerConfiguration.builder(pullSubscribeOptions.getConsumerConfiguration());
            ccBuilder.deliverSubject(null); // pull mode can't have a deliver subject
        }
        else {
            so = pushSubscribeOptions == null
                    ? PushSubscribeOptions.builder().build()
                    : pushSubscribeOptions;
            stream = so.getStream(); // might be null, that's ok
            ccBuilder = ConsumerConfiguration.builder(so.getConsumerConfiguration());
        }

        String durable = ccBuilder.getDurable();
        String inbox = ccBuilder.getDeliverSubject();
        String filterSubject = ccBuilder.getFilterSubject();

        boolean createConsumer = true;

        // 1. Did they tell me what stream? No? look it up
        if (stream == null) {
            stream = lookupStreamBySubject(subject);
        }

        // 2. Is this a durable or ephemeral
        if (durable != null) {
            ConsumerInfo consumerInfo =
                    lookupConsumerInfo(stream, durable);

            if (consumerInfo != null) { // consumer for that durable already exists
                createConsumer = false;
                ConsumerConfiguration cc = consumerInfo.getConsumerConfiguration();

                // Make sure the subject matches or is a subset...
                String existingFilterSubject = cc.getFilterSubject();
                if (filterSubject != null && !filterSubject.equals(existingFilterSubject)) {
                    throw new IllegalArgumentException(
                            String.format("Subject %s mismatches consumer configuration %s.", subject, filterSubject));
                }

                filterSubject = existingFilterSubject;

                // use the deliver subject as the inbox. It may be null, that's ok
                inbox = cc.getDeliverSubject();
            }
        }

        // 3. If no deliver subject (inbox) provided or found, make an inbox.
        if (inbox == null) {
            inbox = conn.createInbox();
        }

        // 4. create the subscription
        NatsJetStreamSubscription sub;
        if (dispatcher == null) {
            sub = (NatsJetStreamSubscription) conn.createSubscription(inbox, queueName, null, true);
        }
        else {
            MessageHandler mh;
            if (autoAck) {
                mh = new AutoAckMessageHandler(handler);
            } else {
                mh = handler;
            }
            sub = (NatsJetStreamSubscription) dispatcher.subscribeImpl(inbox, queueName, mh, true);
        }

        // 5-Consumer didn't exist. It's either ephemeral or a durable that didn't already exist.
        if (createConsumer) {
            // Defaults should set the right ack pending.
            // if we have acks and the maxAckPending is not set, set it
            // to the internal Max.
            // TODO: too high value?
            if (ccBuilder.getMaxAckPending() == 0
                    && ccBuilder.getAckPolicy() != AckPolicy.None) {
                ccBuilder.maxAckPending(sub.getPendingMessageLimit());
            }

            // Pull mode doesn't maintain a deliver subject. It's actually an error if we send it.
            if (!isPullMode) {
                ccBuilder.deliverSubject(inbox);
            }

            // being discussed if this is correct, but leave it for now.
            ccBuilder.filterSubject(filterSubject == null ? subject : filterSubject);

            // createOrUpdateConsumer can fail for security reasons, maybe other reasons?
            ConsumerInfo ci;
            try {
                ci = addOrUpdateConsumerInternal(stream, ccBuilder.build());
            } catch (JetStreamApiException e) {
                if (dispatcher == null) {
                    sub.unsubscribe();
                }
                else {
                    dispatcher.unsubscribe(sub);
                }
                throw e;
            }
            sub.setupJetStream(this, ci.getName(), ci.getStreamName(), inbox, so);
        }
        // 5-Consumer did exist.
        else {
            sub.setupJetStream(this, durable, stream, inbox, so);
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
        queue = validateQueueName(queue, false);
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
        queue = validateQueueName(queue, false);
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
        validateNotNull(options.getDurable(), "Durable");
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
            if (e.getErrorCode() == 404 && e.getErrorDescription().contains("consumer")) {
                return null;
            }
            throw e;
        }
    }

    private String lookupStreamBySubject(String subject) throws IOException, JetStreamApiException {
        String streamRequest = String.format("{\"subject\":\"%s\"}", subject);
        StreamNamesReader snr = new StreamNamesReader();
        Message resp = makeRequestResponseRequired(JSAPI_STREAMS, streamRequest.getBytes(), jso.getRequestTimeout());
        snr.process(resp);
        if (snr.getStrings().size() != 1) {
            throw new IllegalStateException("No matching streams for subject: " + subject);
        }
        return snr.getStrings().get(0);
    }

    private static class AutoAckMessageHandler implements MessageHandler {
        MessageHandler userMH;

        // caller must ensure userMH is not null
        AutoAckMessageHandler(MessageHandler userMH) {
            this.userMH = userMH;
        }

        @Override
        public void onMessage(Message msg) throws InterruptedException {
            try  {
                userMH.onMessage(msg);
                msg.ack();
            } catch (Exception e) {
                // TODO ignore??  schedule async error?
            }
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // Request Utils
    // ----------------------------------------------------------------------------------------------------
    private Message makeRequest(String subject, byte[] bytes, Duration timeout) throws InterruptedException {
        return conn.request(prependPrefix(subject), bytes, timeout);
    }

    private Message makeRequestResponseRequired(String subject, byte[] bytes, Duration timeout) throws IOException {
        try {
            return responseRequired(conn.request(prependPrefix(subject), bytes, timeout));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    private Message makeRequestResponseRequired(String subject, Headers headers, byte[] data, boolean utf8mode, Duration timeout, boolean cancelOn503) throws IOException {
        try {
            return responseRequired(conn.requestInternal(subject, headers, data, utf8mode, timeout, cancelOn503));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    private Message responseRequired(Message respMessage) throws IOException {
        if (respMessage == null) {
            throw new IOException("Timeout or no response waiting for NATS JetStream server");
        }
        return respMessage;
    }

    String prependPrefix(String subject) {
        return jso.getPrefix() + subject;
    }

    Duration getRequestTimeout() {
        return jso.getRequestTimeout();
    }
}
