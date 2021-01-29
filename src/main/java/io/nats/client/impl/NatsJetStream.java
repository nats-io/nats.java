package io.nats.client.impl;

import io.nats.client.*;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static io.nats.client.support.Validator.*;

public class NatsJetStream implements JetStream, JetStreamManagement, NatsJetStreamConstants {

    private static final PublishOptions DEFAULT_PUB_OPTS = PublishOptions.builder().build();
    private static final Duration DEFAULT_TIMEOUT = Options.DEFAULT_CONNECTION_TIMEOUT;

    private static final String msgIdHdr             = "Nats-Msg-Id";
    private static final String expectedStreamHdr    = "Nats-Expected-Stream";
    private static final String expectedLastSeqHdr   = "Nats-Expected-Last-Sequence";
    private static final String expectedLastMsgIdHdr = "Nats-Expected-Last-Msg-Id";

    private final NatsConnection conn;
    private final String prefix;
    private final boolean direct;
    private final JetStreamOptions options;

    private static boolean isJetStreamEnabled(Message msg) {
        if (msg == null) {
            return false;
        }

        JetStreamApiResponse apiResp = new JetStreamApiResponse(msg);
        return apiResp.getCode() != 503 && apiResp.getError() == null;
    }

    NatsJetStream(NatsConnection connection, JetStreamOptions jsOptions) throws IOException {
        options = jsOptions == null
                ? JetStreamOptions.builder().build()
                : jsOptions;

        conn = connection;
        prefix = options.getPrefix();
        direct = options.isDirectMode();

        // override request style.
        conn.getOptions().setOldRequestStyle(true);

        if (!direct) {
            Message resp = makeRequest(JSAPI_ACCOUNT_INFO, null, DEFAULT_TIMEOUT);
            if (!isJetStreamEnabled(resp)) {
                throw new IllegalStateException("JetStream is not enabled.");
            }

            // check the response
            new NatsJetStreamAccountStats(new String(resp.getData()));
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

        Message resp = makeRequest(subj, requestJSON.getBytes(), conn.getOptions().getConnectionTimeout());
        return new ConsumerInfo(extractApiResponse(resp).getResponse());
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
        Message resp = makeRequest(subj, config.toJSON().getBytes(), DEFAULT_TIMEOUT);
        return new StreamInfo(extractApiResponse(resp).getResponse());
    }

    @Override
    public void deleteStream(String streamName) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_STREAM_DELETE, streamName);
        extractApiResponse( makeRequest(subj, null, DEFAULT_TIMEOUT) );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamInfo streamInfo(String streamName) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_STREAM_INFO, streamName);
        Message resp = makeRequest(subj, null, DEFAULT_TIMEOUT);
        return new StreamInfo(extractApiResponseJson(resp));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamInfo purgeStream(String streamName) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_STREAM_PURGE, streamName);
        Message resp = makeRequest(subj, null, DEFAULT_TIMEOUT);
        return new StreamInfo(extractApiResponseJson(resp));
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
        extractApiResponse( makeRequest(subj, null, DEFAULT_TIMEOUT) );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerLister getConsumers(String streamName) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_CONSUMER_LIST, streamName);
        Message resp = makeRequest(subj, null, DEFAULT_TIMEOUT);
        return new ConsumerLister(extractApiResponseJson(resp));
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

        PublishOptions opts;
        if (options == null) {
            opts = DEFAULT_PUB_OPTS;
        } else {
            opts = options;

            Headers headers = natsMessage.getOrCreateHeaders();

            // we know no headers are set with default options
            long seqno = opts.getExpectedLastSequence();
            if (seqno > 0) {
                headers.add(expectedLastSeqHdr, Long.toString(seqno));
            }

            String s = opts.getExpectedLastMsgId();
            if (s != null) {
                headers.add(expectedLastMsgIdHdr, s);
            }

            s = opts.getExpectedStream();
            if (s != null) {
                headers.add(expectedStreamHdr, s);
            }

            s = opts.getMessageId();
            if (s != null) {
                headers.add(msgIdHdr, s);
            }
        }

        Message resp = makeRequest(natsMessage, opts.getStreamTimeout());
        NatsPublishAck ack = new NatsPublishAck(resp.getData());

        String ackStream = ack.getStream();
        if (ackStream == null || ackStream.length() == 0 || ack.getSeqno() == 0) {
            throw new IOException("Invalid JetStream ack.");
        }

        String pubStream = opts.getStream();
        if (isStreamSpecified(pubStream) && !pubStream.equals(ackStream)) {
            throw new IOException("Expected ack from stream " + pubStream + ", received from: " + ackStream);
        }

        return ack;
    }

    private boolean isStreamSpecified(String streamName) {
        return streamName != null;
    }

    ConsumerInfo getConsumerInfo(String stream, String consumer) throws IOException, JetStreamApiException {
        String ccInfoSubj = String.format(JSAPI_CONSUMER_INFO, stream, consumer);
        Message resp = makeRequest(ccInfoSubj, null, DEFAULT_TIMEOUT);
        return new ConsumerInfo(extractApiResponseJson(resp));
    }

    private String lookupStreamBySubject(String subject) throws IOException, JetStreamApiException {
        if (subject == null) {
            throw new IllegalArgumentException("subject cannot be null.");
        }
        String streamRequest = String.format("{\"subject\":\"%s\"}", subject);

        Message resp = makeRequest(JSAPI_STREAMS, streamRequest.getBytes(), DEFAULT_TIMEOUT);

        String[] streams = JsonUtils.parseStringArray("streams", extractApiResponseJson(resp));
        if (streams.length != 1) {
            throw new IllegalStateException("No matching streams.");
        }
        return streams[0];
    }

    private static class AutoAckMessageHandler implements MessageHandler {
        MessageHandler mh;

        // caller must ensure userMH is not null
        AutoAckMessageHandler(MessageHandler userMH) {
            mh = userMH;
        }

        @Override
        public void onMessage(Message msg) throws InterruptedException {
            try  {
                mh.onMessage(msg);
                msg.ack();
            } catch (Exception e) {
                // TODO ignore??  schedule async error?
            }
        }
    }

    NatsJetStreamSubscription createSubscription(String subject, String queueName,
                                                 NatsDispatcher dispatcher, MessageHandler handler,
                                                 SubscribeOptions options) throws IOException, JetStreamApiException {

        // setup the configuration, use a default.
        SubscribeOptions scribeOps = SubscribeOptions.createOrCopy(options);
        ConsumerConfiguration suppliedCnsmrCnfg = scribeOps.getConsumerConfiguration();

        boolean isPullMode = (scribeOps.getPullBatchSize() > 0);
        if (handler != null && isPullMode) {
            throw new IllegalStateException("Pull mode is not allowed with dispatcher.");
        }

        // If you have (a stream AND a consumer) or (a deliver subject)
        boolean shouldAttach = (scribeOps.getStream() != null && scribeOps.getConsumer() != null)
                            || (suppliedCnsmrCnfg.getDeliverSubject() != null);
        boolean shouldCreate = !shouldAttach;

        if (direct && shouldCreate) {
            throw new IllegalStateException("Direct mode is required.");
        }

        String deliverSubject;
        String stream = null;

        if (direct) {
            // make sure there is a place to deliver
            String temp = scribeOps.getConsumerConfiguration().getDeliverSubject();
            deliverSubject = temp == null ? conn.createInbox() : temp;
        }
        else if (shouldAttach) {
            ConsumerConfiguration attachedCnsmrCnfg = getConsumerInfo(scribeOps.getStream(), scribeOps.getConsumer()).getConsumerConfiguration();

            // Make sure the subject matches or is a subset...
            String filterSub = attachedCnsmrCnfg.getFilterSubject();
            if (filterSub != null && !filterSub.equals(subject)) {
                throw new IllegalArgumentException(
                        String.format("Subject %s mismatches consumer configuration %s.", subject, filterSub));
            }

            String temp = attachedCnsmrCnfg.getDeliverSubject();
            deliverSubject = temp == null ? conn.createInbox() : temp;
        }
        else {
            stream = lookupStreamBySubject(subject);
            deliverSubject = conn.createInbox();
            if (!isPullMode) {
                suppliedCnsmrCnfg.setDeliverSubject(deliverSubject);
            }
            suppliedCnsmrCnfg.setFilterSubject(subject);
        }

        NatsJetStreamSubscription sub;
        if (dispatcher != null) {
            MessageHandler mh;
            if (scribeOps.isAutoAck()) {
                mh = new AutoAckMessageHandler(handler);
            } else {
                mh = handler;
            }
            sub = (NatsJetStreamSubscription) dispatcher.subscribeImpl(deliverSubject, queueName, mh, true);
        } else {
            sub = (NatsJetStreamSubscription) conn.createSubscription(deliverSubject, queueName, dispatcher, true);
        }

        // if we're updating or creating the consumer, give it a go here.
        if (shouldCreate) {
            // Defaults should set the right ack pending.

            // if we have acks and the maxAckPending is not set, set it
            // to the internal Max.
            // TODO: too high value?
            if (suppliedCnsmrCnfg.getMaxAckPending() == 0) {
                suppliedCnsmrCnfg.setMaxAckPending(sub.getPendingMessageLimit());
            }

            ConsumerInfo ci = null;
            try {
                ci = createOrUpdateConsumer(stream, suppliedCnsmrCnfg);
            } catch (JetStreamApiException e) {
                sub.unsubscribe();
                throw e;
            }
            sub.setupJetStream(this, ci.getName(), ci.getStreamName(), deliverSubject, scribeOps.getPullBatchSize());
        }
        else {
            sub.setupJetStream(this, scribeOps.getConsumer(), scribeOps.getStream(), deliverSubject, scribeOps.getPullBatchSize());
        }

        if (isPullMode) {
            sub.poll();
        }

        return sub;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject) throws IOException, JetStreamApiException {
        validateJsSubscribeSubject(subject);
        return createSubscription(subject, null, null, null, SubscribeOptions.builder().build());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, SubscribeOptions options) throws IOException, JetStreamApiException {
        validateJsSubscribeSubject(subject);
        validateNotNull(options, "options");
        return createSubscription(subject, null, null, null, options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, String queue, SubscribeOptions options) throws IOException, JetStreamApiException {
        validateJsSubscribeSubject(subject);
        validateQueueNameRequired(queue);
        validateNotNull(options, "options");
        return createSubscription(subject, queue, null, null, options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, Dispatcher dispatcher, MessageHandler handler) throws IOException, JetStreamApiException {
        validateJsSubscribeSubject(subject);
        validateNotNull(dispatcher, "dispatcher");
        validateNotNull(handler, "handler");
        return createSubscription(subject, null, (NatsDispatcher) dispatcher, handler, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, Dispatcher dispatcher, MessageHandler handler, SubscribeOptions options) throws IOException, JetStreamApiException {
        validateJsSubscribeSubject(subject);
        validateNotNull(dispatcher, "dispatcher");
        validateNotNull(handler, "handler");
        validateNotNull(options, "options");
        return createSubscription(subject, null, (NatsDispatcher) dispatcher, handler, options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, String queue, Dispatcher dispatcher, MessageHandler handler) throws IOException, JetStreamApiException {
        validateJsSubscribeSubject(subject);
        validateQueueNameRequired(queue);
        validateNotNull(dispatcher, "dispatcher");
        validateNotNull(handler, "handler");
        return createSubscription(subject, queue, (NatsDispatcher) dispatcher, handler, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamSubscription subscribe(String subject, String queue, Dispatcher dispatcher, MessageHandler handler, SubscribeOptions options) throws IOException, JetStreamApiException {
        validateJsSubscribeSubject(subject);
        validateQueueNameRequired(queue);
        validateNotNull(dispatcher, "dispatcher");
        validateNotNull(handler, "handler");
        validateNotNull(options, "options");
        return createSubscription(subject, queue, (NatsDispatcher) dispatcher, handler, options);
    }

    private Message makeRequest(String subject, byte[] bytes, Duration timeout) throws IOException {
        try {
            return responseRequired(conn.request(appendPrefix(subject), bytes, timeout));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    private Message makeRequest(NatsMessage natsMessage, Duration timeout) throws IOException {
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

    private String extractApiResponseJson(Message respMessage) throws JetStreamApiException {
        return extractApiResponse(respMessage).getResponse();
    }

    private JetStreamApiResponse extractApiResponse(Message respMessage) throws JetStreamApiException {
        JetStreamApiResponse jsApiResp = new JetStreamApiResponse(respMessage);
        if (jsApiResp.hasError()) {
            throw new JetStreamApiException(jsApiResp);
        }
        return jsApiResp;
    }

    String appendPrefix(String subject) {
        if (prefix == null) {
            return JSAPI_DEFAULT_PREFIX + subject;
        }
        return prefix + subject;
    }
}
