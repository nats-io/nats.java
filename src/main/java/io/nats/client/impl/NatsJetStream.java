package io.nats.client.impl;

import io.nats.client.*;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.nats.client.impl.JsonUtils.buildNumberPattern;
import static io.nats.client.support.Validator.*;

public class NatsJetStream implements JetStream {

    private static final String JSAPI_DEFAULT_PREFIX = "$JS.API.";

    // JSAPI_ACCOUNT_INFO is for obtaining general information about JetStream.
    private static final String JSAPI_ACCOUNT_INFO = "INFO";

    // JSAPI_CONSUMER_CREATE is used to create consumers.
    private static final String JSAPI_CONSUMER_CREATE = "CONSUMER.CREATE.%s";

    // JSAPI_DURABLE_CREATE is used to create durable consumers.
    private static final String JSAPI_DURABLE_CREATE = "CONSUMER.DURABLE.CREATE.%s.%s";

    // JSAPI_CONSUMER_INFO is used to create consumers.
    private static final String JSAPI_CONSUMER_INFO = "CONSUMER.INFO.%s.%s";

    // JSAPI_CONSUMER_REQUEST_NEXT is the prefix for the request next message(s) for a consumer in worker/pull mode.
    private static final String JSAPI_CONSUMER_REQUEST_NEXT = "CONSUMER.MSG.NEXT.%s.%s";

    // JSAPI_CONSUMER_DELETE is used to delete consumers.
    private static final String JSAPI_CONSUMER_DELETE = "CONSUMER.DELETE.%s.%s";

    // JSAPI_CONSUMER_LIST is used to return all detailed consumer information
    private static final String JSAPI_CONSUMER_LIST = "CONSUMER.LIST.%s";

    // JSAPI_STREAMS can lookup a stream by subject.
    private static final String JSAPI_STREAMS = "STREAM.NAMES";

    // JSAPI_STREAM_CREATE is the endpoint to create new streams.
    private static final String JSAPI_STREAM_CREATE = "STREAM.CREATE.%s";

    // JSAPI_STREAM_INFO is the endpoint to get information on a stream.
    private static final String JSAPI_STREAM_INFO = "STREAM.INFO.%s";

    // JSAPI_STREAM_UPDATE is the endpoint to update existing streams.
    private static final String JSAPI_STREAM_UPDATE = "STREAM.UPDATE.%s";

    // JSAPI_STREAM_DELETE is the endpoint to delete streams.
    private static final String JSAPI_STREAM_DELETE = "STREAM.DELETE.%s";

    // JSAPI_STREAM_PURGE is the endpoint to purge streams.
    private static final String JSAPI_STREAM_PURGE = "STREAM.PURGE.%s";

    // JSAPI_STREAM_LIST is the endpoint that will return all detailed stream information
    private static final String JSAPI_STREAM_LIST = "STREAM.LIST";

    // JSAPI_MSG_DELETE is the endpoint to remove a message.
    private static final String JSAPI_MSG_DELETE = "STREAM.MSG.DELETE.%s";

    private static final PublishOptions DEFAULT_PUB_OPTS = PublishOptions.builder().build();
    private static final Duration defaultTimeout = Options.DEFAULT_CONNECTION_TIMEOUT;

    private static final String msgIdHdr             = "Nats-Msg-Id";
    private static final String expectedStreamHdr    = "Nats-Expected-Stream";
    private static final String expectedLastSeqHdr   = "Nats-Expected-Last-Sequence";
    private static final String expectedLastMsgIdHdr = "Nats-Expected-Last-Msg-Id";

    private static final Pattern LIMITS_MEMORY_RE = buildNumberPattern("max_memory");
    private static final Pattern LIMITS_STORAGE_RE = buildNumberPattern("max_storage");
    private static final Pattern LIMIT_STREAMS_RE = buildNumberPattern("max_streams");
    private static final Pattern LIMIT_CONSUMERS_RE = buildNumberPattern("max_consumers");

    private static final Pattern STATS_MEMORY_RE = buildNumberPattern("memory");
    private static final Pattern STATS_STORAGE_RE = buildNumberPattern("storage");
    private static final Pattern STATS_STREAMS_RE = buildNumberPattern("streams");
    private static final Pattern STATS_CONSUMERS_RE = buildNumberPattern("consumers");

    private final NatsConnection conn;
    private final String prefix;
    private final boolean direct;
    private final JetStreamOptions options;

    public static class AccountLimitImpl implements AccountLimits {
        long memory = -1;
        long storage = -1;
        long streams = -1;
        long consumers = 1;

        AccountLimitImpl(String json) {
            Matcher m = LIMITS_MEMORY_RE.matcher(json);
            if (m.find()) {
                this.memory = Long.parseLong(m.group(1));
            }

            m = LIMITS_STORAGE_RE.matcher(json);
            if (m.find()) {
                this.storage = Long.parseLong(m.group(1));
            }

            m = LIMIT_STREAMS_RE.matcher(json);
            if (m.find()) {
                this.streams = Long.parseLong(m.group(1));
            }

            m = LIMIT_CONSUMERS_RE.matcher(json);
            if (m.find()) {
                this.consumers = Long.parseLong(m.group(1));
            }
        }

        @Override
        public long getMaxMemory() {
            return memory;
        }

        @Override
        public long getMaxStorage() {
            return storage;
        }

        @Override
        public long getMaxStreams() {
            return streams;
        }

        @Override
        public long getMaxConsumers() {
            return consumers;
        }

        @Override
        public String toString() {
            return "AccountLimitImpl{" +
                    "memory=" + memory +
                    ", storage=" + storage +
                    ", streams=" + streams +
                    ", consumers=" + consumers +
                    '}';
        }
    }

    public static class AccountStatsImpl implements AccountStatistics {
        long memory = -1;
        long storage = -1;
        long streams = -1;
        long consumers = 1;

        AccountStatsImpl(String json) {
            Matcher m = STATS_MEMORY_RE.matcher(json);
            if (m.find()) {
                this.memory = Long.parseLong(m.group(1));
            }

            m = STATS_STORAGE_RE.matcher(json);
            if (m.find()) {
                this.storage = Long.parseLong(m.group(1));
            }

            m = STATS_STREAMS_RE.matcher(json);
            if (m.find()) {
                this.streams = Long.parseLong(m.group(1));
            }

            m = STATS_CONSUMERS_RE.matcher(json);
            if (m.find()) {
                this.consumers = Long.parseLong(m.group(1));
            }
        }
        @Override
        public long getMemory() {
            return memory;
        }

        @Override
        public long getStorage() {
            return storage;
        }

        @Override
        public long getStreams() {
            return streams;
        }

        @Override
        public long getConsumers() {
            return consumers;
        }

        @Override
        public String toString() {
            return "AccountStatsImpl{" +
                    "memory=" + memory +
                    ", storage=" + storage +
                    ", streams=" + streams +
                    ", consumers=" + consumers +
                    '}';
        }
    }

    private static boolean isJetstreamEnabled(Message msg) {
        if (msg == null) {
            return false;
        }

        JetstreamAPIResponse apiResp = new JetstreamAPIResponse(msg);
        return apiResp.getCode() != 503 && apiResp.getError() == null;
    }

    NatsJetStream(NatsConnection connection, JetStreamOptions jsOptions) throws InterruptedException, TimeoutException {
        if (jsOptions == null) {
            options = JetStreamOptions.builder().build();
        } else {
            options = jsOptions;
        }
        conn = connection;
        prefix = options.getPrefix();
        direct = options.isDirectMode();

        // override request style.
        conn.getOptions().setOldRequestStyle(true);

        if (!direct) {
            Message resp = makeRequest(JSAPI_ACCOUNT_INFO, null, defaultTimeout);
            if (!isJetstreamEnabled(resp)) {
                throw new IllegalStateException("Jetstream is not enabled.");
            }

            // check the response
            new AccountStatsImpl(new String(resp.getData()));
        }
    }

    private ConsumerInfo createOrUpdateConsumer(String streamName, ConsumerConfiguration config) throws TimeoutException, InterruptedException, IOException {
        String durable = config.getDurable();
        String requestJSON = config.toJSON(streamName);

        String subj;
        if (durable == null) {
            subj = String.format(JSAPI_CONSUMER_CREATE, streamName);
        } else {
            subj = String.format(JSAPI_DURABLE_CREATE, streamName, durable);
        }

        Message resp = makeRequest(subj, requestJSON.getBytes(), conn.getOptions().getConnectionTimeout());
        return new ConsumerInfo(jsApiReponseOrIoEx(resp).getResponse());
    }

    @Override
    public StreamInfo addStream(StreamConfiguration config) throws TimeoutException, InterruptedException {
        return _addOrUpdate(config, JSAPI_STREAM_CREATE, "create");
    }

    @Override
    public StreamInfo updateStream(StreamConfiguration config) throws TimeoutException, InterruptedException {
        return _addOrUpdate(config, JSAPI_STREAM_UPDATE, "update");
    }

    private StreamInfo _addOrUpdate(StreamConfiguration config, String template, String note) throws InterruptedException, TimeoutException {
        if (config == null) {
            throw new IllegalArgumentException("configuration cannot be null.");
        }
        String streamName = config.getName();
        if (streamName == null || streamName.isEmpty()) {
            throw new IllegalArgumentException("Configuration must have a valid name");
        }

        String subj = String.format(template, streamName);
        Message resp = makeRequest(subj, config.toJSON().getBytes(), defaultTimeout);
        return new StreamInfo(jsApiReponseOrStateEx(resp, note).getResponse());
    }

    @Override
    public void deleteStream(String streamName) throws TimeoutException, InterruptedException {
        String subj = String.format(JSAPI_STREAM_DELETE, streamName);
        Message resp = makeRequest(subj, null, defaultTimeout);
        jsApiReponseOrStateEx(resp, "delete");
    }

    @Override
    public StreamInfo streamInfo(String streamName) throws TimeoutException, InterruptedException {
        String subj = String.format(JSAPI_STREAM_INFO, streamName);
        Message resp = makeRequest(subj, null, defaultTimeout);
        return new StreamInfo(jsApiReponseOrStateEx(resp, "get info for").getResponse());
    }

    @Override
    public StreamInfo purgeStream(String streamName) throws TimeoutException, InterruptedException {
        String subj = String.format(JSAPI_STREAM_PURGE, streamName);
        Message resp = makeRequest(subj, null, defaultTimeout);
        return new StreamInfo(jsApiReponseOrStateEx(resp, "purge").getResponse());
    }

    @Override
    public ConsumerInfo addConsumer(String streamName, ConsumerConfiguration config) throws InterruptedException, IOException, TimeoutException {
        validateStreamName(streamName);
        validateNotNull(config, "config");
        return addConsumer(null, streamName, config);
    }

    private ConsumerInfo addConsumer(String subject, String stream, ConsumerConfiguration config) throws InterruptedException, IOException, TimeoutException {
        validateStreamName(stream);
        validateNotNull(config, "config");
        if (provided(subject)) {
            config.setDeliverSubject(subject);
        }
        return createOrUpdateConsumer(stream, config);
    }

    @Override
    public void deleteConsumer(String streamName, String consumer) throws TimeoutException, InterruptedException, IOException {
        String subj = String.format(JSAPI_CONSUMER_DELETE, streamName, consumer);
        Message resp = makeRequest(subj, null, defaultTimeout);
        jsApiReponseOrIoEx(resp);
    }

    @Override
    public ConsumerLister newConsumerLister(String streamName) throws TimeoutException, InterruptedException, IOException {
        String subj = String.format(JSAPI_CONSUMER_LIST, streamName);
        Message resp = makeRequest(subj, null, defaultTimeout);
        return new ConsumerLister(jsApiReponseOrIoEx(resp).getResponse());
    }

    static NatsMessage buildMsg(String subject, byte[] payload) {
        return new NatsMessage.Builder().subject(subject).data(payload).build();
    }

    @Override
    public PublishAck publish(String subject, byte[] body) throws IOException, InterruptedException, TimeoutException {
        return publishInternal(buildMsg(subject, body), null);
    }

    @Override
    public PublishAck publish(String subject, byte[] body, PublishOptions options) throws IOException, InterruptedException, TimeoutException{
        return publishInternal(buildMsg(subject, body), options);
    }

    @Override
    public PublishAck publish(Message message) throws IOException, InterruptedException, TimeoutException {
        return publishInternal(message, null);
    }

    @Override
    public PublishAck publish(Message message, PublishOptions options) throws IOException, InterruptedException, TimeoutException{
        return publishInternal(message, options);
    }

    @Override
    public CompletableFuture<PublishAck> publishAsync(String subject, byte[] body) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return publish(subject, body);
            } catch (IOException | InterruptedException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public CompletableFuture<PublishAck> publishAsync(String subject, byte[] body, PublishOptions options) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return publish(subject, body, options);
            } catch (IOException | InterruptedException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public CompletableFuture<PublishAck> publishAsync(Message message) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return publish(message);
            } catch (IOException | InterruptedException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public CompletableFuture<PublishAck> publishAsync(Message message, PublishOptions options) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return publish(message, options);
            } catch (IOException | InterruptedException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private PublishAck publishInternal(Message message, PublishOptions options) throws IOException, InterruptedException, TimeoutException{
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
            throw new IOException("Invalid jetstream ack.");
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

    ConsumerInfo getConsumerInfo(String stream, String consumer) throws TimeoutException, InterruptedException {
        String ccInfoSubj = String.format(JSAPI_CONSUMER_INFO, stream, consumer);
        Message resp = makeRequest(ccInfoSubj, null, defaultTimeout);
        return new ConsumerInfo(jsApiReponseOrStateEx(resp).getResponse());
    }

    private String lookupStreamBySubject(String subject) throws InterruptedException, IOException, TimeoutException {
        if (subject == null) {
            throw new IllegalArgumentException("subject cannot be null.");
        }
        String streamRequest = String.format("{\"subject\":\"%s\"}", subject);

        Message resp = makeRequest(JSAPI_STREAMS, streamRequest.getBytes(), defaultTimeout);

        JetstreamAPIResponse jsResp = jsApiReponseOrIoEx(resp);
        String[] streams = JsonUtils.parseStringArray("streams", jsResp.getResponse());
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
                // ignore??  schedule async error?
            }
        }
    }

    NatsJetStreamSubscription createSubscription(String subject, String queueName, NatsDispatcher dispatcher, MessageHandler handler, SubscribeOptions options) throws InterruptedException, TimeoutException, IOException{

        // setup the configuration, use a default.
        SubscribeOptions o = SubscribeOptions.getInstance(options);
        ConsumerConfiguration cfg = o.getConsumerConfiguration();

        boolean isPullMode = (o.getPullBatchSize() > 0);
        if (handler != null && isPullMode) {
            throw new IllegalStateException("Pull mode is not allowed with dispatcher.");
        }

        boolean shouldAttach = o.getStream() != null && o.getConsumer() != null || o.getConsumerConfiguration().getDeliverSubject() != null;
        boolean shouldCreate = !shouldAttach;

        if (this.direct && shouldCreate) {
            throw new IllegalStateException("Direct mode is required.");
        }

        String deliver = null;
        String stream = null;
        ConsumerConfiguration ccfg = null;

        if (direct) {
            String s = o.getConsumerConfiguration().getDeliverSubject();
            if (s == null) {
                deliver = conn.createInbox();
            } else {
                deliver = s;
            }
        } else if (shouldAttach) {
            ccfg = getConsumerInfo(o.getStream(), o.getConsumer()).getConsumerConfiguration();

            // Make sure the subject matches or is a subset...
            if (ccfg.getFilterSubject() != null && !ccfg.getFilterSubject().equals(subject)) {
                throw new IllegalArgumentException(String.format("Subject %s mismatches consumer configuration %s.",
                        subject, ccfg.getFilterSubject()));
            }

            String s = ccfg.getDeliverSubject();
            deliver = s != null ? s : conn.createInbox();
        } else {
            stream = lookupStreamBySubject(subject);
            deliver = conn.createInbox();
            if (!isPullMode) {
                cfg.setDeliverSubject(deliver);
            }
            cfg.setFilterSubject(subject);
        }

        NatsJetStreamSubscription sub;
        if (dispatcher != null) {
            MessageHandler mh;
            if (options == null || options.isAutoAck()) {
                mh = new AutoAckMessageHandler(handler);
            } else {
                mh = handler;
            }
            sub = (NatsJetStreamSubscription) dispatcher.subscribeImpl(deliver, queueName, mh, true);
        } else {
            sub = (NatsJetStreamSubscription) conn.createSubscription(deliver, queueName, dispatcher, true);
        }

        // if we're updating or creating the consumer, give it a go here.
        if (shouldCreate) {
            // Defaults should set the right ack pending.

            // if we have acks and the maxAckPending is not set, set it
            // to the internal Max.
            // TODO:  too high value?
            if (cfg.getMaxAckPending() == 0) {
                cfg.setMaxAckPending(sub.getPendingMessageLimit());
            }

            try  {
                ConsumerInfo ci = createOrUpdateConsumer(stream, cfg);
                sub.setupJetStream(this, ci.getName(), ci.getStreamName(),
                        deliver, o.getPullBatchSize());
            } catch (Exception e) {
                sub.unsubscribe();
                throw e;
            }
        } else {
            String s = direct ? o.getConsumerConfiguration().getDeliverSubject() : ccfg.getDeliverSubject();
            if (s == null) {
                s = deliver;
            }
            sub.setupJetStream(this, o.getConsumer(), o.getStream(), s, o.getPullBatchSize());
        }

        if (isPullMode) {
            sub.poll();
        }

        return sub;
    }

    @Override
    public JetStreamSubscription subscribe(String subject) throws InterruptedException, TimeoutException, IOException {
        validateJsSubscribeSubject(subject);
        return createSubscription(subject, null, null, null, SubscribeOptions.builder().build());
    }

    @Override
    public JetStreamSubscription subscribe(String subject, SubscribeOptions options) throws InterruptedException, TimeoutException, IOException {
        validateJsSubscribeSubject(subject);
        validateNotNull(options, "options");
        return createSubscription(subject, null, null, null, options);
    }

    @Override
    public JetStreamSubscription subscribe(String subject, String queue, SubscribeOptions options) throws InterruptedException, TimeoutException, IOException {
        validateJsSubscribeSubject(subject);
        validateQueueName(queue);
        validateNotNull(options, "options");
        return createSubscription(subject, queue, null, null, options);
    }

    @Override
    public JetStreamSubscription subscribe(String subject, Dispatcher dispatcher, MessageHandler handler) throws InterruptedException, TimeoutException, IOException {
        validateJsSubscribeSubject(subject);
        validateNotNull(dispatcher, "dispatcher");
        validateNotNull(handler, "handler");
        return createSubscription(subject, null, (NatsDispatcher) dispatcher, handler, null);
    }

    @Override
    public JetStreamSubscription subscribe(String subject, Dispatcher dispatcher, MessageHandler handler, SubscribeOptions options) throws InterruptedException, TimeoutException, IOException {
        validateJsSubscribeSubject(subject);
        validateNotNull(dispatcher, "dispatcher");
        validateNotNull(handler, "handler");
        validateNotNull(options, "options");
        return createSubscription(subject, null, (NatsDispatcher) dispatcher, handler, options);
    }

    @Override
    public JetStreamSubscription subscribe(String subject, String queue, Dispatcher dispatcher, MessageHandler handler) throws InterruptedException, TimeoutException, IOException {
        validateJsSubscribeSubject(subject);
        validateQueueName(queue);
        validateNotNull(dispatcher, "dispatcher");
        validateNotNull(handler, "handler");
        return createSubscription(subject, queue, (NatsDispatcher) dispatcher, handler, null);
    }

    @Override
    public JetStreamSubscription subscribe(String subject, String queue, Dispatcher dispatcher, MessageHandler handler, SubscribeOptions options) throws InterruptedException, TimeoutException, IOException {
        validateJsSubscribeSubject(subject);
        validateQueueName(queue);
        validateNotNull(dispatcher, "dispatcher");
        validateNotNull(handler, "handler");
        validateNotNull(options, "options");
        return createSubscription(subject, queue, (NatsDispatcher) dispatcher, handler, options);
    }

    private Message makeRequest(String subject, byte[] bytes, Duration timeout) throws InterruptedException, TimeoutException {
        return checkMessage(conn.request(appendPre(subject), bytes, timeout));
    }

    private Message makeRequest(NatsMessage natsMessage, Duration timeout) throws InterruptedException, TimeoutException {
        return checkMessage(conn.request(natsMessage, timeout));
    }

    private Message checkMessage(Message respMessage) throws TimeoutException {
        if (respMessage == null) {
            throw new TimeoutException("Timeout or no response waiting for NATS Jetstream server");
        }
        return respMessage;
    }

    private JetstreamAPIResponse jsApiReponseOrStateEx(Message msg, String note) {
        JetstreamAPIResponse jsApiResp = new JetstreamAPIResponse(msg);
        if (jsApiResp.hasError()) {
            throw new IllegalStateException(String.format("Could not %s stream. %d : %s",
                    note, jsApiResp.getCode(), jsApiResp.getDescription()));
        }
        return jsApiResp;
    }

    private JetstreamAPIResponse jsApiReponseOrStateEx(Message msg) {
        JetstreamAPIResponse jsApiResp = new JetstreamAPIResponse(msg);
        if (jsApiResp.hasError()) {
            throw new IllegalStateException(jsApiResp.getError());
        }
        return jsApiResp;
    }

    private JetstreamAPIResponse jsApiReponseOrIoEx(Message msg) throws IOException {
        JetstreamAPIResponse jsApiResp = new JetstreamAPIResponse(msg);
        if (jsApiResp.hasError()) {
            throw new IOException(jsApiResp.getError());
        }
        return jsApiResp;
    }

    private String appendPre(String template, Object... args) {
        return appendPre(String.format(template, args));
    }

    String appendPre(String subject) {
        if (prefix == null) {
            return JSAPI_DEFAULT_PREFIX + subject;
        }
        return prefix + subject;
    }
}
