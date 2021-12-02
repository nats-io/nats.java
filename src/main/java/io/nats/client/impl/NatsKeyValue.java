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

import static io.nats.client.api.MessageGetRequest.lastBySubjectBytes;
import static io.nats.client.support.NatsJetStreamConstants.JSAPI_MSG_GET;
import static io.nats.client.support.NatsJetStreamConstants.JS_NO_MESSAGE_FOUND_ERR;
import static io.nats.client.support.NatsKeyValueUtil.*;
import static io.nats.client.support.Validator.*;

public class NatsKeyValue implements KeyValue {

    private final String bucketName;
    private final String stream;
    private final NatsJetStream js;
    private final JetStreamManagement jsm;

    public NatsKeyValue(NatsConnection connection, String bucketName, JetStreamOptions options) throws IOException {
        this.bucketName = Validator.validateKvBucketNameRequired(bucketName);
        stream = streamName(this.bucketName);
        js = new NatsJetStream(connection, options);
        jsm = new NatsJetStreamManagement(connection, options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getBucketName() {
        return bucketName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] getValue(String key) throws IOException, JetStreamApiException {
        KeyValueEntry entry = getEntry(key);
        return entry == null ? null : entry.getValue();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getStringValue(String key) throws IOException, JetStreamApiException {
        KeyValueEntry entry = getEntry(key);
        return entry == null ? null : entry.getValueAsString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long getLongValue(String key) throws IOException, JetStreamApiException {
        KeyValueEntry entry = getEntry(key);
        return entry == null ? null : entry.getValueAsLong();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KeyValueEntry getEntry(String key) throws IOException, JetStreamApiException {
        validateNonWildcardKvKeyRequired(key);
        String subj = String.format(JSAPI_MSG_GET, stream);
        Message resp = js.makeRequestResponseRequired(subj, lastBySubjectBytes(keySubject(bucketName, key)), JetStreamOptions.DEFAULT_TIMEOUT);
        MessageInfo mi = new MessageInfo(resp);
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
        validateNonWildcardKvKeyRequired(key);
        PublishAck pa = js.publish(NatsMessage.builder()
                .subject(keySubject(bucketName, key))
                .data(value)
                .build());
        return pa.getSeqno();
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
    public long put(String key, long value) throws IOException, JetStreamApiException {
        return put(key, Long.toString(value).getBytes(StandardCharsets.US_ASCII));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(String key) throws IOException, JetStreamApiException {
        _deletePurge(key, DELETE_HEADERS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void purge(String key) throws IOException, JetStreamApiException {
        _deletePurge(key, PURGE_HEADERS);
    }

    private void _deletePurge(String key, Headers h) throws IOException, JetStreamApiException {
        validateNonWildcardKvKeyRequired(key);
        js.publish(NatsMessage.builder().subject(keySubject(bucketName, key)).headers(h).build());
    }

    @Override
    public NatsKeyValueWatchSubscription watch(String key, KeyValueWatcher watcher, boolean metaOnly, KeyValueOperation... operations) throws IOException, JetStreamApiException, InterruptedException {
        validateWildcardKvKeyRequired(key);
        validateNotNull(watcher, "Watcher is required");
        return new NatsKeyValueWatchSubscription(js, bucketName, key, metaOnly, watcher, operations);
    }

    @Override
    public NatsKeyValueWatchSubscription watchAll(KeyValueWatcher watcher, boolean metaOnly, KeyValueOperation... operations) throws IOException, JetStreamApiException, InterruptedException {
        validateNotNull(watcher, "Watcher is required");
        return new NatsKeyValueWatchSubscription(js, bucketName, ">", metaOnly, watcher, operations);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> keys() throws IOException, JetStreamApiException, InterruptedException {
        List<String> list = new ArrayList<>();
        visitSubject(streamSubject(bucketName), DeliverPolicy.LastPerSubject, true, false, m -> {
            KeyValueOperation op = getOperation(m.getHeaders(), KeyValueOperation.PUT);
            if (!op.isDelete()) {
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
        visitSubject(keySubject(bucketName, key), DeliverPolicy.All, false, true, m -> {
            list.add(new KeyValueEntry(m));
        });
        return list;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void purgeDeletes()  throws IOException, JetStreamApiException, InterruptedException {
        List<String> list = new ArrayList<>();
        visitSubject(streamSubject(bucketName), DeliverPolicy.LastPerSubject, true, false, m -> {
            KeyValueOperation op = getOperation(m.getHeaders(), KeyValueOperation.PUT);
            if (op.isDelete()) {
                list.add(new BucketAndKey(m).key);
            }
        });

        for (String key : list) {
            jsm.purgeStream(stream, PurgeOptions.subject(keySubject(bucketName, key)));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KeyValueStatus getStatus() throws IOException, JetStreamApiException, InterruptedException {
        return new KeyValueStatus(jsm.getStreamInfo(streamName(bucketName)));
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
        Message m = sub.nextMessage(Duration.ofMillis(5000)); // give plenty of time for the first
        while (m != null) {
            handler.onMessage(m);
            m = sub.nextMessage(Duration.ofMillis(100)); // the rest should come pretty quick
        }
        sub.unsubscribe();
    }
}
