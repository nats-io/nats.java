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
import io.nats.client.support.DateTimeUtils;
import io.nats.client.support.Validator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZonedDateTime;
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
    private final String rawKeyPrefix;
    private final String pubSubKeyPrefix;

    NatsKeyValue(NatsConnection connection, String bucketName, KeyValueOptions kvo) throws IOException {
        this.bucketName = Validator.validateKvBucketNameRequired(bucketName);
        streamName = toStreamName(bucketName);
        streamSubject = toStreamSubject(bucketName);
        rawKeyPrefix = toKeyPrefix(bucketName);
        if (kvo == null) {
            js = new NatsJetStream(connection, null);
            jsm = new NatsJetStreamManagement(connection, null);
            pubSubKeyPrefix = rawKeyPrefix;
        }
        else {
            js = new NatsJetStream(connection, kvo.getJetStreamOptions());
            jsm = new NatsJetStreamManagement(connection, kvo.getJetStreamOptions());
            if (kvo.getJetStreamOptions().isDefaultPrefix()) {
                pubSubKeyPrefix = rawKeyPrefix;
            }
            else {
                pubSubKeyPrefix = kvo.getJetStreamOptions().getPrefix() + rawKeyPrefix;
            }
        }
    }

    String rawKeySubject(String key) {
        return rawKeyPrefix + key;
    }

    String pubSubKeySubject(String key) {
        return pubSubKeyPrefix + key;
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

    /**
     * {@inheritDoc}
     */
    @Override
    public KeyValueEntry get(String key) throws IOException, JetStreamApiException {
        return _kvGetLastMessage(validateNonWildcardKvKeyRequired(key));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KeyValueEntry get(String key, long revision) throws IOException, JetStreamApiException {
        return _kvGetMessage(validateNonWildcardKvKeyRequired(key), revision);
    }

    KeyValueEntry _kvGetLastMessage(String key) throws IOException, JetStreamApiException {
        try {
            return new KeyValueEntry(jsm.getLastMessage(streamName, rawKeySubject(key)));
        }
        catch (JetStreamApiException jsae) {
            if (jsae.getApiErrorCode() == JS_NO_MESSAGE_FOUND_ERR) {
                return null;
            }
            throw jsae;
        }
    }

    KeyValueEntry _kvGetMessage(String key, long revision) throws IOException, JetStreamApiException {
        try {
            KeyValueEntry kve = new KeyValueEntry(jsm.getMessage(streamName, revision));
            return key.equals(kve.getKey()) ? kve : null;
        }
        catch (JetStreamApiException jsae) {
            if (jsae.getApiErrorCode() == JS_NO_MESSAGE_FOUND_ERR) {
                return null;
            }
            throw jsae;
        }
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
        validateNonWildcardKvKeyRequired(key);
        try {
            return update(key, value, 0);
        }
        catch (JetStreamApiException e) {
            if (e.getApiErrorCode() == JS_WRONG_LAST_SEQUENCE) {
                // must check if the last message for this subject is a delete or purge
                KeyValueEntry kve = _kvGetLastMessage(key);
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
        validateNonWildcardKvKeyRequired(key);
        Headers h = new Headers().add(EXPECTED_LAST_SUB_SEQ_HDR, Long.toString(expectedRevision));
        return _publishWithNonWildcardKey(key, value, h).getSeqno();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(String key) throws IOException, JetStreamApiException {
        validateNonWildcardKvKeyRequired(key);
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
        return js.publish(NatsMessage.builder().subject(pubSubKeySubject(key)).data(data).headers(h).build());
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
        visitSubject(rawKeySubject(">"), DeliverPolicy.LastPerSubject, true, false, m -> {
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
        visitSubject(rawKeySubject(key), DeliverPolicy.All, false, true, m -> list.add(new KeyValueEntry(m)));
        return list;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void purgeDeletes()  throws IOException, JetStreamApiException, InterruptedException {
        purgeDeletes(null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void purgeDeletes(KeyValuePurgeOptions options) throws IOException, JetStreamApiException, InterruptedException {
        long dmThresh = options == null
            ? KeyValuePurgeOptions.DEFAULT_THRESHOLD_MILLIS
            : options.getDeleteMarkersThresholdMillis();

        ZonedDateTime limit;
        if (dmThresh < 0) {
            limit = DateTimeUtils.fromNow(600000); // long enough in the future to clear all
        }
        else if (dmThresh == 0) {
            limit = DateTimeUtils.fromNow(KeyValuePurgeOptions.DEFAULT_THRESHOLD_MILLIS);
        }
        else {
            limit = DateTimeUtils.fromNow(-dmThresh);
        }

        List<String> keep0List = new ArrayList<>();
        List<String> keep1List = new ArrayList<>();
        visitSubject(streamSubject, DeliverPolicy.LastPerSubject, true, false, m -> {
            KeyValueEntry kve = new KeyValueEntry(m);
            if (kve.getOperation() != KeyValueOperation.PUT) {
                if (kve.getCreated().isAfter(limit)) {
                    keep1List.add(new BucketAndKey(m).key);
                }
                else {
                    keep0List.add(new BucketAndKey(m).key);
                }
            }
        });

        for (String key : keep0List) {
            jsm.purgeStream(streamName, PurgeOptions.subject(rawKeySubject(key)));
        }

        for (String key : keep1List) {
            PurgeOptions po = PurgeOptions.builder()
                .subject(rawKeySubject(key))
                .keep(1)
                .build();
            jsm.purgeStream(streamName, po);
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

        Duration timeout = js.jso.getRequestTimeout();
        JetStreamSubscription sub = js.subscribe(subject, pso);
        try {
            boolean lastWasNull = false;
            long pending = sub.getConsumerInfo().getCalculatedPending();
            while (pending > 0) { // no need to loop if nothing pending
                Message m = sub.nextMessage(timeout);
                if (m == null) {
                    if (lastWasNull) {
                        return;
                    }
                    lastWasNull = true;
                }
                else {
                    handler.onMessage(m);
                    if (--pending == 0) {
                        return;
                    }
                    lastWasNull = false;
                }
            }
        }
        finally {
            sub.unsubscribe();
        }
    }
}
