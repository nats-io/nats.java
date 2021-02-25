package io.nats.client.impl;

import io.nats.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.nats.client.support.Validator.*;

public class NatsJetStream implements JetStream, JetStreamManagement, NatsJetStreamConstants {

    private final NatsConnection conn;
    private final String prefix;
    private final Duration requestTimeout;

    // ----------------------------------------------------------------------------------------------------
    // Create / Init
    // ----------------------------------------------------------------------------------------------------
    NatsJetStream(NatsConnection connection, JetStreamOptions jsOptions) throws IOException {
        conn = connection;
        if (jsOptions == null) {
            prefix = null;
            requestTimeout = JetStreamOptions.DEFAULT_TIMEOUT;
        }
        else {
            prefix = jsOptions.getPrefix();
            requestTimeout = jsOptions.getRequestTimeout();
        }

        checkEnabled();
    }

    private void checkEnabled() throws IOException {
        try {
            JetStreamApiResponse jsApiResp = null;
            Message respMessage = conn.request(appendPrefix(JSAPI_ACCOUNT_INFO), null, requestTimeout);
            if (respMessage != null) {
                jsApiResp = new JetStreamApiResponse(respMessage);
            }
            if (jsApiResp == null || (jsApiResp.hasError() && jsApiResp.getErrorCode() == 503)) {
                throw new IllegalStateException("JetStream is not enabled.");
            }

            // check the response // TODO find out why this is being done
            new NatsJetStreamAccountStats(jsApiResp.getResponse());

        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    private ConsumerInfo createOrUpdateConsumer(String streamName, ConsumerConfiguration config) throws IOException, JetStreamApiException {
        String durable = config.getDurable();
        String requestJSON = config.toJSON(streamName);

        String subj;
        if (durable == null) {
            subj = String.format(JSAPI_CONSUMER_CREATE, streamName);
        } else {
            subj = String.format(JSAPI_DURABLE_CREATE, streamName, durable);
        }
        Message resp = makeRequestResponseRequired(subj, requestJSON.getBytes(), conn.getOptions().getConnectionTimeout());
        return new ConsumerInfo(extractApiResponseThrowOnError(resp).getResponse());
    }

    // ----------------------------------------------------------------------------------------------------
    // Manage
    // ----------------------------------------------------------------------------------------------------

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamInfo addStream(StreamConfiguration config) throws IOException, JetStreamApiException {
        return _addOrUpdate(config, JSAPI_STREAM_CREATE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamInfo updateStream(StreamConfiguration config) throws IOException, JetStreamApiException {
        return _addOrUpdate(config, JSAPI_STREAM_UPDATE);
    }

    private StreamInfo _addOrUpdate(StreamConfiguration config, String template) throws IOException, JetStreamApiException {
        if (config == null) {
            throw new IllegalArgumentException("Configuration cannot be null.");
        }
        String streamName = config.getName();
        if (nullOrEmpty(streamName)) {
            throw new IllegalArgumentException("Configuration must have a valid stream name");
        }

        String subj = String.format(template, streamName);
        Message resp = makeRequestResponseRequired(subj, config.toJSON().getBytes(), requestTimeout);
        return new StreamInfo(extractApiResponseThrowOnError(resp).getResponse());
    }

    @Override
    public void deleteStream(String streamName) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_STREAM_DELETE, streamName);
        extractApiResponseThrowOnError( makeRequestResponseRequired(subj, null, requestTimeout) );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamInfo getStreamInfo(String streamName) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_STREAM_INFO, streamName);
        Message resp = makeRequestResponseRequired(subj, null, requestTimeout);
        return new StreamInfo(extractJsonThrowOnError(resp));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PurgeResponse purgeStream(String streamName) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_STREAM_PURGE, streamName);
        Message resp = makeRequestResponseRequired(subj, null, requestTimeout);
        return new PurgeResponse(extractJsonThrowOnError(resp));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerInfo addConsumer(String streamName, ConsumerConfiguration config) throws IOException, JetStreamApiException {
        validateStreamNameRequired(streamName);
        validateNotNull(config, "Config");
        validateNotNull(config.getDurable(), "Durable");
        return createOrUpdateConsumer(streamName, config);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteConsumer(String streamName, String consumer) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_CONSUMER_DELETE, streamName, consumer);
        extractApiResponseThrowOnError( makeRequestResponseRequired(subj, null, requestTimeout) );
    }

    @Override
    public ConsumerInfo getConsumerInfo(String streamName, String consumer) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_CONSUMER_INFO, streamName, consumer);
        Message resp = makeRequestResponseRequired(subj, null, requestTimeout);
        return new ConsumerInfo(extractJsonThrowOnError(resp));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getConsumerNames(String streamName) throws IOException, JetStreamApiException {
        return getConsumerNames(streamName, null);
    }

    @Override
    public List<String> getConsumerNames(String streamName, String filter) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_CONSUMER_NAMES, streamName);

        ConsumerNamesResponse cnr = new ConsumerNamesResponse();
        while (cnr.hasMore()) {
            Message resp = makeRequestResponseRequired(subj, cnr.nextJson(filter).getBytes(StandardCharsets.US_ASCII), requestTimeout);
            cnr.add(extractJsonThrowOnError(resp));
        }

        return cnr.getConsumers();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<ConsumerInfo> getConsumers(String streamName) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_CONSUMER_LIST, streamName);

        ConsumerListResponse cir = new ConsumerListResponse();
        while (cir.hasMore()) {
            Message resp = makeRequestResponseRequired(subj, cir.nextJson().getBytes(StandardCharsets.US_ASCII), requestTimeout);
            cir.add(extractJsonThrowOnError(resp));
        }

        return cir.getConsumers();
    }

    @Override
    public List<String> getStreamNames() throws IOException, JetStreamApiException {
        StreamNamesResponse snr = new StreamNamesResponse();
        while (snr.hasMore()) {
            Message resp = makeRequestResponseRequired(JSAPI_STREAMS, snr.nextJson().getBytes(StandardCharsets.US_ASCII), requestTimeout);
            snr.add(extractJsonThrowOnError(resp));
        }

        return snr.getStreams();
    }

    @Override
    public List<StreamInfo> getStreams() throws IOException, JetStreamApiException {
        StreamListResponse sir = new StreamListResponse();
        while (sir.hasMore()) {
            Message resp = makeRequestResponseRequired(JSAPI_STREAM_LIST, sir.nextJson().getBytes(StandardCharsets.US_ASCII), requestTimeout);
            sir.add(extractJsonThrowOnError(resp));
        }

        return sir.getStreams();
    }

    // ----------------------------------------------------------------------------------------------------
    // Publish
    // ---------------------------------------------------------------------------------------------NatsJsPullSub-------
    static NatsMessage buildNatsMessage(String subject, byte[] payload) {
        return new NatsMessage.Builder().subject(subject).data(payload).build();
    }

    static NatsMessage toNatsMessage(Message message) {
        validateNotNull(message, "Message");
        return message instanceof NatsMessage ? (NatsMessage) message : new NatsMessage(message);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PublishAck publish(String subject, byte[] body) throws IOException, JetStreamApiException {
        return publishInternal(buildNatsMessage(subject, body), null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PublishAck publish(String subject, byte[] body, PublishOptions options) throws IOException, JetStreamApiException {
        return publishInternal(buildNatsMessage(subject, body), options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PublishAck publish(Message message) throws IOException, JetStreamApiException {
        return publish(toNatsMessage(message), null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PublishAck publish(Message message, PublishOptions options) throws IOException, JetStreamApiException {
        return publishInternal(toNatsMessage(message), options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<PublishAck> publishAsync(String subject, byte[] body) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return publish(subject, body);
            } catch (IOException | JetStreamApiException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<PublishAck> publishAsync(String subject, byte[] body, PublishOptions options) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return publish(subject, body, options);
            } catch (IOException | JetStreamApiException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<PublishAck> publishAsync(Message message) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return publish(message);
            } catch (IOException | JetStreamApiException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<PublishAck> publishAsync(Message message, PublishOptions options) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return publish(message, options);
            } catch (IOException | JetStreamApiException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private PublishAck publishInternal(NatsMessage natsMessage, PublishOptions options) throws IOException, JetStreamApiException {

        Duration timeout;
        String pubStream = null;

        if (options == null) {
            timeout = requestTimeout;
        }
        else {
            timeout = options.getStreamTimeout();
            pubStream = options.getStream();

            Headers headers = natsMessage.getOrCreateHeaders();

            // we know no headers are set with default options
            long seqno = options.getExpectedLastSequence();
            if (seqno > 0) {
                headers.add(EXPECTED_LAST_SEQ_HDR, Long.toString(seqno));
            }

            String s = options.getExpectedLastMsgId();
            if (s != null) {
                headers.add(EXPECTED_LAST_MSG_ID_HDR, s);
            }

            s = options.getExpectedStream();
            if (s != null) {
                headers.add(EXPECTED_STREAM_HDR, s);
            }

            s = options.getMessageId();
            if (s != null) {
                headers.add(MSG_ID_HDR, s);
            }
        }

        Message resp = makeRequestResponseRequired(natsMessage, timeout);
        NatsPublishAck ack = new NatsPublishAck(resp.getData());

        String ackStream = ack.getStream();
        if (ackStream == null || ackStream.length() == 0 || ack.getSeqno() == 0) {
            throw new IOException("Invalid JetStream ack.");
        }

        if (isStreamSpecified(pubStream) && !pubStream.equals(ackStream)) {
            throw new IOException("Expected ack from stream " + pubStream + ", received from: " + ackStream);
        }

        return ack;
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

        if (isPullMode) {
            stream = pullSubscribeOptions.getStream();
            ccBuilder = ConsumerConfiguration.builder(pullSubscribeOptions.getConsumerConfiguration());
            ccBuilder.deliverSubject(null); // pull mode can't have a deliver subject
        }
        else if (pushSubscribeOptions == null) {
            stream = null;
            ccBuilder = ConsumerConfiguration.builder();
        }
        else {
            stream = pushSubscribeOptions.getStream(); // might be null, that's ok
            ccBuilder = ConsumerConfiguration.builder(pushSubscribeOptions.getConsumerConfiguration());
        }

        String durable = ccBuilder.getDurable();
        String inbox = ccBuilder.getDeliverSubject();

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
                String filterSub = cc.getFilterSubject();
                if (filterSub != null && !filterSub.equals(subject)) {
                    throw new IllegalArgumentException(
                            String.format("Subject %s mismatches consumer configuration %s.", subject, filterSub));
                }

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
            if (ccBuilder.getMaxAckPending() == 0) {
                ccBuilder.maxAckPending(sub.getPendingMessageLimit());
            }

            // Pull mode doesn't maintain a deliver subject. It's actually an error if we send it.
            if (!isPullMode) {
                ccBuilder.deliverSubject(inbox);
            }

            // being discussed if this is correct, but leave it for now.
            ccBuilder.filterSubject(subject);

            // createOrUpdateConsumer can fail for security reasons, maybe other reasons?
            ConsumerInfo ci;
            try {
                ci = createOrUpdateConsumer(stream, ccBuilder.build());
            } catch (JetStreamApiException e) {
                sub.unsubscribe();
                throw e;
            }
            sub.setupJetStream(this, ci.getName(), ci.getStreamName(), inbox, pullSubscribeOptions);
        }
        // 5-Consumer did exist.
        else {
            sub.setupJetStream(this, durable, stream, inbox, pullSubscribeOptions);
        }

        return sub;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject) throws IOException, JetStreamApiException {
        validateJsSubscribeSubjectRequired(subject);
        return createSubscription(subject, null, null, null, false, null, null);
    }

    @Override
    public JetStreamSubscription subscribe(String subject, PushSubscribeOptions options) throws IOException, JetStreamApiException {
        validateJsSubscribeSubjectRequired(subject);
        return createSubscription(subject, null, null, null, false, options, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, String queue, PushSubscribeOptions options) throws IOException, JetStreamApiException {
        validateJsSubscribeSubjectRequired(subject);
        String qOrNull = validateQueueNameOrEmptyAsNull(queue);
        return createSubscription(subject, qOrNull, null, null, false, options, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, Dispatcher dispatcher, MessageHandler handler, boolean autoAck) throws IOException, JetStreamApiException {
        validateJsSubscribeSubjectRequired(subject);
        validateNotNull(dispatcher, "Dispatcher");
        validateNotNull(handler, "Handler");
        return createSubscription(subject, null, (NatsDispatcher) dispatcher, handler, autoAck, null, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, Dispatcher dispatcher, MessageHandler handler, boolean autoAck, PushSubscribeOptions options) throws IOException, JetStreamApiException {
        validateJsSubscribeSubjectRequired(subject);
        validateNotNull(dispatcher, "Dispatcher");
        validateNotNull(handler, "Handler");
        return createSubscription(subject, null, (NatsDispatcher) dispatcher, handler, autoAck, options, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, String queue, Dispatcher dispatcher, MessageHandler handler, boolean autoAck, PushSubscribeOptions options) throws IOException, JetStreamApiException {
        validateJsSubscribeSubjectRequired(subject);
        String qOrNull = validateQueueNameOrEmptyAsNull(queue);
        validateNotNull(dispatcher, "Dispatcher");
        validateNotNull(handler, "Handler");
        return createSubscription(subject, qOrNull, (NatsDispatcher) dispatcher, handler, autoAck, options, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, PullSubscribeOptions options) throws IOException, JetStreamApiException {
        validateJsSubscribeSubjectRequired(subject);
        validateNotNull(options, "Options");
        validateNotNull(options.getDurable(), "Durable");
        return createSubscription(subject, null, null, null, false, null, options);
    }

    // ----------------------------------------------------------------------------------------------------
    // General Utils
    // ----------------------------------------------------------------------------------------------------
    private boolean isStreamSpecified(String streamName) {
        return streamName != null;
    }

    ConsumerInfo lookupConsumerInfo(String stream, String consumer) throws IOException, JetStreamApiException {
        try {
            return getConsumerInfo(stream, consumer);
        }
        catch (JetStreamApiException e) {
            if (e.getErrorCode() == 404) {
                return null;
            }
            throw e;
        }
    }

    private String lookupStreamBySubject(String subject) throws IOException, JetStreamApiException {
        String streamRequest = String.format("{\"subject\":\"%s\"}", subject);

        Message resp = makeRequestResponseRequired(JSAPI_STREAMS, streamRequest.getBytes(), requestTimeout);

        String[] streams = JsonUtils.getStringArray("streams", extractJsonThrowOnError(resp));
        if (streams.length != 1) {
            throw new IllegalStateException("No matching streams.");
        }
        return streams[0];
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
    private Message makeRequestResponseRequired(String subject, byte[] bytes, Duration timeout) throws IOException {
        try {
            return responseRequired(conn.request(appendPrefix(subject), bytes, timeout));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    private Message makeRequestResponseRequired(NatsMessage natsMessage, Duration timeout) throws IOException {
        try {
            return responseRequired(conn.request(natsMessage, timeout));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    private Message responseRequired(Message respMessage) throws IOException {
        if (respMessage == null) {
            throw new IOException("Timeout or no response waiting for NATS Jetstream server");
        }
        return respMessage;
    }

    private String extractJsonThrowOnError(Message resp) throws JetStreamApiException {
        return extractApiResponseThrowOnError(resp).getResponse();
    }

    private JetStreamApiResponse extractApiResponseThrowOnError(Message respMessage) throws JetStreamApiException {
        JetStreamApiResponse jsApiResp = extractApiResponse(respMessage);
        if (jsApiResp.hasError()) {
            throw new JetStreamApiException(jsApiResp);
        }
        return jsApiResp;
    }

    private JetStreamApiResponse extractApiResponse(Message respMessage) {
        return new JetStreamApiResponse(respMessage);
    }

    String appendPrefix(String subject) {
        if (prefix == null) {
            return JSAPI_DEFAULT_PREFIX + subject;
        }
        return prefix + subject;
    }
}
