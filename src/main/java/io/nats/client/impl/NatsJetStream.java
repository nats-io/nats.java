package io.nats.client.impl;

import io.nats.client.*;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static io.nats.client.support.Validator.*;

public class NatsJetStream implements JetStream, JetStreamManagement, NatsJetStreamConstants {

    private static final String msgIdHdr             = "Nats-Msg-Id";
    private static final String expectedStreamHdr    = "Nats-Expected-Stream";
    private static final String expectedLastSeqHdr   = "Nats-Expected-Last-Sequence";
    private static final String expectedLastMsgIdHdr = "Nats-Expected-Last-Msg-Id";

    private final NatsConnection conn;
    private final String prefix;
    private final Duration requestTimeout;

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

        // override request style.
        conn.getOptions().setOldRequestStyle(true);

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
            throw new IllegalArgumentException("configuration cannot be null.");
        }
        String streamName = config.getName();
        if (nullOrEmpty(streamName)) {
            throw new IllegalArgumentException("Configuration must have a valid name");
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
    public StreamInfo streamInfo(String streamName) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_STREAM_INFO, streamName);
        Message resp = makeRequestResponseRequired(subj, null, requestTimeout);
        return new StreamInfo(extractJsonThrowOnError(resp));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamInfo purgeStream(String streamName) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_STREAM_PURGE, streamName);
        Message resp = makeRequestResponseRequired(subj, null, requestTimeout);
        return new StreamInfo(extractJsonThrowOnError(resp));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerInfo addConsumer(String streamName, ConsumerConfiguration config) throws IOException, JetStreamApiException {
        validateStreamNameRequired(streamName);
        validateNotNull(config, "config");
        return addConsumer(null, streamName, config);
    }

    private ConsumerInfo addConsumer(String subject, String stream, ConsumerConfiguration config) throws IOException, JetStreamApiException {
        validateStreamNameRequired(stream);
        validateNotNull(config, "config");
        if (!nullOrEmpty(subject)) {
            config.setDeliverSubject(subject);
        }
        return createOrUpdateConsumer(stream, config);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteConsumer(String streamName, String consumer) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_CONSUMER_DELETE, streamName, consumer);
        extractApiResponseThrowOnError( makeRequestResponseRequired(subj, null, requestTimeout) );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerLister getConsumers(String streamName) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_CONSUMER_LIST, streamName);
        Message resp = makeRequestResponseRequired(subj, null, requestTimeout);
        return new ConsumerLister(extractJsonThrowOnError(resp));
    }

    static NatsMessage buildMsg(String subject, byte[] payload) {
        return new NatsMessage.Builder().subject(subject).data(payload).build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PublishAck publish(String subject, byte[] body) throws IOException, JetStreamApiException {
        return publishInternal(buildMsg(subject, body), null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PublishAck publish(String subject, byte[] body, PublishOptions options) throws IOException, JetStreamApiException {
        return publishInternal(buildMsg(subject, body), options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PublishAck publish(Message message) throws IOException, JetStreamApiException {
        return publishInternal(message, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PublishAck publish(Message message, PublishOptions options) throws IOException, JetStreamApiException {
        return publishInternal(message, options);
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

    private PublishAck publishInternal(Message message, PublishOptions options) throws IOException {
        validateNotNull(message, "message");

        NatsMessage natsMessage = message instanceof NatsMessage ? (NatsMessage)message : new NatsMessage(message);

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
                headers.add(expectedLastSeqHdr, Long.toString(seqno));
            }

            String s = options.getExpectedLastMsgId();
            if (s != null) {
                headers.add(expectedLastMsgIdHdr, s);
            }

            s = options.getExpectedStream();
            if (s != null) {
                headers.add(expectedStreamHdr, s);
            }

            s = options.getMessageId();
            if (s != null) {
                headers.add(msgIdHdr, s);
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

    private boolean isStreamSpecified(String streamName) {
        return streamName != null;
    }

    ConsumerInfo lookupConsumerInfo(String stream, String consumer) throws IOException, JetStreamApiException {
        String ccInfoSubj = String.format(JSAPI_CONSUMER_INFO, stream, consumer);
        Message resp = makeRequestResponseRequired(ccInfoSubj, null, requestTimeout);
        JetStreamApiResponse jsApiResp = extractApiResponse(resp);
        if (jsApiResp.hasError()) {
            if (jsApiResp.getErrorCode() == 404) {
                return null;
            }
            throw new JetStreamApiException(jsApiResp);
        }
        return new ConsumerInfo(extractJsonThrowOnError(resp));
    }

    private String lookupStreamBySubject(String subject) throws IOException, JetStreamApiException {
        if (subject == null) {
            throw new IllegalArgumentException("subject cannot be null.");
        }
        String streamRequest = String.format("{\"subject\":\"%s\"}", subject);

        Message resp = makeRequestResponseRequired(JSAPI_STREAMS, streamRequest.getBytes(), requestTimeout);

        String[] streams = JsonUtils.parseStringArray("streams", extractJsonThrowOnError(resp));
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

    NatsJetStreamSubscription createSubscription(String subject, String queueName,
                                                 NatsDispatcher dispatcher, MessageHandler handler, boolean autoAck,
                                                 PushSubscribeOptions pushSubscribeOptions,
                                                 PullSubscribeOptions pullSubscribeOptions) throws IOException, JetStreamApiException {
        // first things first...
        boolean isPullMode = pullSubscribeOptions != null;
        if (handler != null && isPullMode) {
            throw new IllegalStateException("Pull mode is not allowed with dispatcher.");
        }

        // setup the configuration, use a default.
        String stream;
        ConsumerConfiguration workingCC;
        int defaultBatchSize = 0;
        boolean defaultNoWait = false;

        if (isPullMode) {
            stream = pullSubscribeOptions.getStream();
            workingCC = pullSubscribeOptions.getConsumerConfiguration();
            defaultBatchSize = pullSubscribeOptions.getDefaultBatchSize();
            defaultNoWait = pullSubscribeOptions.getDefaultNoWait();
        }
        else if (pushSubscribeOptions == null) {
            stream = null;
            workingCC = ConsumerConfiguration.defaultConfiguration();
        }
        else {
            stream = pushSubscribeOptions.getStream();
            workingCC = pushSubscribeOptions.getConsumerConfiguration();
        }
        String durable = workingCC.getDurable();
        String inbox = workingCC.getDeliverSubject();
        boolean shouldCreate = true;

        // 1. Did they tell me what stream? No? look it up
        if (stream == null) {
            stream = lookupStreamBySubject(subject);
        }

        // 2. Is this a durable or ephemeral
        if (durable != null) {
            ConsumerInfo consumerInfo =
                    lookupConsumerInfo(stream, durable);

            if (consumerInfo != null) {
                ConsumerConfiguration cc = consumerInfo.getConsumerConfiguration();
                // Make sure the subject matches or is a subset...
                String filterSub = cc.getFilterSubject();
                if (filterSub != null && !filterSub.equals(subject)) {
                    throw new IllegalArgumentException(
                            String.format("Subject %s mismatches consumer configuration %s.", subject, filterSub));
                }

                // since we found a valid config for the durable, it existed.
                // Make the config the working one.
                workingCC = cc;
                inbox = workingCC.getDeliverSubject();
                shouldCreate = false;
            }
        }

        // 3. No deliver subject (inbox) provided or found in existing config? Make an inbox
        if (inbox == null) {
            inbox = conn.createInbox();
        }

        // 4. Pull mode doesn't maintain a deliver subject. It's actually an error if we send it.
        if (isPullMode) {
            workingCC.setDeliverSubject(null);
        }
        else {
            workingCC.setDeliverSubject(inbox);
        }

        // 5.
        workingCC.setFilterSubject(subject);

        // 6.
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

        // 7-Create. Creating the consumer. It either isn't durable or a duable that didn't already exist.
        if (shouldCreate) {
            // Defaults should set the right ack pending.
            // if we have acks and the maxAckPending is not set, set it
            // to the internal Max.
            // TODO: too high value?
            if (workingCC.getMaxAckPending() == 0) {
                workingCC.setMaxAckPending(sub.getPendingMessageLimit());
            }

            // A. createOrUpdateConsumer can fail for security reasons, maybe other reasons?
            ConsumerInfo ci;
            try {
                ci = createOrUpdateConsumer(stream, workingCC);
            } catch (JetStreamApiException e) {
                sub.unsubscribe();
                throw e;
            }
            sub.setupJetStream(this, ci.getName(), ci.getStreamName(), inbox, defaultBatchSize, defaultNoWait);
        }
        // 7-Exists.
        else {
            sub.setupJetStream(this, durable, stream, inbox, defaultBatchSize, defaultNoWait);
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
        validateNotNull(dispatcher, "dispatcher");
        validateNotNull(handler, "handler");
        return createSubscription(subject, null, (NatsDispatcher) dispatcher, handler, autoAck, null, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, Dispatcher dispatcher, MessageHandler handler, boolean autoAck, PushSubscribeOptions options) throws IOException, JetStreamApiException {
        validateJsSubscribeSubjectRequired(subject);
        validateNotNull(dispatcher, "dispatcher");
        validateNotNull(handler, "handler");
        return createSubscription(subject, null, (NatsDispatcher) dispatcher, handler, autoAck, options, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, String queue, Dispatcher dispatcher, MessageHandler handler, boolean autoAck, PushSubscribeOptions options) throws IOException, JetStreamApiException {
        validateJsSubscribeSubjectRequired(subject);
        String qOrNull = validateQueueNameOrEmptyAsNull(queue);
        validateNotNull(dispatcher, "dispatcher");
        validateNotNull(handler, "handler");
        return createSubscription(subject, null, (NatsDispatcher) dispatcher, handler, autoAck, options, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, PullSubscribeOptions options) throws IOException, JetStreamApiException {
        validateJsSubscribeSubjectRequired(subject);
        validateNotNull(options, "options");
        validateNotNull(options.getDurable(), "durable");
        return createSubscription(subject, null, null, null, false, null, options);
    }

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
