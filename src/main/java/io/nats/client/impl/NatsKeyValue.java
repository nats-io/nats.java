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
import io.nats.client.support.Validator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static io.nats.client.support.NatsJetStreamConstants.*;
import static io.nats.client.support.NatsKeyValueUtil.*;
import static io.nats.client.support.Validator.*;

public class NatsKeyValue implements KeyValue {

    final NatsJetStream js;
    final JetStreamManagement jsm;

    private final String bucketName;
    private final String streamName;
    private final String streamSubject;
    private final String defaultKeyPrefix;
    private final String publishKeyPrefix;

    NatsKeyValue(NatsConnection connection, String bucketName, JetStreamOptions options) throws IOException {
        this.bucketName = Validator.validateKvBucketNameRequired(bucketName);
        streamName = toStreamName(bucketName);
        streamSubject = toStreamSubject(bucketName);
        js = new NatsJetStream(connection, options);
        jsm = new NatsJetStreamManagement(connection, options);
        defaultKeyPrefix = toKeyPrefix(bucketName);
        if (options == null || options.getFeaturePrefix() == null) {
            publishKeyPrefix = defaultKeyPrefix;
        }
        else {
            publishKeyPrefix = options.getFeaturePrefix();
        }
    }

    String defaultKeySubject(String key) {
        return defaultKeyPrefix + key;
    }

    String publishKeySubject(String key) {
        return publishKeyPrefix + key;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getBucketName() {
        return bucketName;
    }

    String getStreamName() {
        return streamName;
    }

    String getStreamSubject() {
        return streamSubject;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KeyValueEntry get(String key) throws IOException, JetStreamApiException {
        return getLastMessage(validateNonWildcardKvKeyRequired(key));
    }

    KeyValueEntry getLastMessage(String key) throws IOException, JetStreamApiException {
        MessageInfo mi = jsm.getLastMessage(streamName, defaultKeySubject(key));
        if (mi.hasError()) {
            if (mi.getApiErrorCode() == JS_NO_MESSAGE_FOUND_ERR) {
                return null; // run of the mill key not found
            }
            mi.throwOnHasError();
        }
        return new KeyValueEntry(mi);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long put(String key, byte[] value) throws IOException, JetStreamApiException {
        return _publishWithNonWildcardKey(key, value, null).getSeqno();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long put(String key, String value) throws IOException, JetStreamApiException {
        return put(key, value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long put(String key, Number value) throws IOException, JetStreamApiException {
        return put(key, value.toString().getBytes(StandardCharsets.US_ASCII));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long create(String key, byte[] value) throws IOException, JetStreamApiException {
        try {
            return update(key, value, 0);
        }
        catch (JetStreamApiException e) {
            if (e.getApiErrorCode() == JS_WRONG_LAST_SEQUENCE) {
                // must check if the last message for this subject is a delete or purge
                KeyValueEntry kve = getLastMessage(key);
                if (kve != null && kve.getOperation() != KeyValueOperation.PUT) {
                    return update(key, value, kve.getRevision());
                }
            }
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long update(String key, byte[] value, long expectedRevision) throws IOException, JetStreamApiException {
        Headers h = new Headers().add(EXPECTED_LAST_SUB_SEQ_HDR, Long.toString(expectedRevision));
        return _publishWithNonWildcardKey(key, value, h).getSeqno();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(String key) throws IOException, JetStreamApiException {
        _publishWithNonWildcardKey(key, null, DELETE_HEADERS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void purge(String key) throws IOException, JetStreamApiException {
        _publishWithNonWildcardKey(key, null, PURGE_HEADERS);
    }

    private PublishAck _publishWithNonWildcardKey(String key, byte[] data, Headers h) throws IOException, JetStreamApiException {
        validateNonWildcardKvKeyRequired(key);
        return js.publish(NatsMessage.builder().subject(publishKeySubject(key)).data(data).headers(h).build());
    }

    @Override
    public NatsKeyValueWatchSubscription watch(String key, KeyValueWatcher watcher, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException {
        validateKvKeyWildcardAllowedRequired(key);
        validateNotNull(watcher, "Watcher is required");
        return new NatsKeyValueWatchSubscription(this, key, watcher, watchOptions);
    }

    @Override
    public NatsKeyValueWatchSubscription watchAll(KeyValueWatcher watcher, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException {
        validateNotNull(watcher, "Watcher is required");
        return new NatsKeyValueWatchSubscription(this, ">", watcher, watchOptions);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> keys() throws IOException, JetStreamApiException, InterruptedException {
        List<String> list = new ArrayList<>();
        visitSubject(defaultKeySubject(">"), DeliverPolicy.LastPerSubject, true, false, m -> {
            KeyValueOperation op = getOperation(m.getHeaders());
            if (op == KeyValueOperation.PUT) {
                list.add(new BucketAndKey(m).key);
            }
        });
        return list;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<KeyValueEntry> history(String key) throws IOException, JetStreamApiException, InterruptedException {
        validateNonWildcardKvKeyRequired(key);
        List<KeyValueEntry> list = new ArrayList<>();
        visitSubject(defaultKeySubject(key), DeliverPolicy.All, false, true, m -> list.add(new KeyValueEntry(m)));
        return list;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void purgeDeletes()  throws IOException, JetStreamApiException, InterruptedException {
        List<String> list = new ArrayList<>();
        visitSubject(streamSubject, DeliverPolicy.LastPerSubject, true, false, m -> {
            KeyValueOperation op = getOperation(m.getHeaders());
            if (op != KeyValueOperation.PUT) {
                list.add(new BucketAndKey(m).key);
            }
        });

        for (String key : list) {
            jsm.purgeStream(streamName, PurgeOptions.subject(defaultKeySubject(key)));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KeyValueStatus getStatus() throws IOException, JetStreamApiException, InterruptedException {
        return new KeyValueStatus(jsm.getStreamInfo(streamName));
    }

    private void visitSubject(String subject, DeliverPolicy deliverPolicy, boolean headersOnly, boolean ordered, MessageHandler handler) throws IOException, JetStreamApiException, InterruptedException {
        PushSubscribeOptions pso = PushSubscribeOptions.builder()
            .ordered(ordered)
            .configuration(
                ConsumerConfiguration.builder()
                    .ackPolicy(AckPolicy.None)
                    .deliverPolicy(deliverPolicy)
                    .headersOnly(headersOnly)
                    .build())
            .build();

        JetStreamSubscription sub = js.subscribe(subject, pso);

        try {
            Duration d100 = Duration.ofMillis(100);
            Message m = sub.nextMessage(Duration.ofMillis(5000)); // give plenty of time for the first
            while (m != null) {
                handler.onMessage(m);
                if (m.metaData().pendingCount() == 0) {
                    m = null;
                }
                else {
                    m = sub.nextMessage(d100); // the rest should come pretty quick
                }
            }
        }
        finally {
            sub.unsubscribe();
        }
    }
}
