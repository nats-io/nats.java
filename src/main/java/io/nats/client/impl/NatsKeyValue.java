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

import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.KeyValueOptions;
import io.nats.client.PurgeOptions;
import io.nats.client.api.*;
import io.nats.client.support.Validator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static io.nats.client.support.NatsJetStreamConstants.EXPECTED_LAST_SUB_SEQ_HDR;
import static io.nats.client.support.NatsJetStreamConstants.JS_WRONG_LAST_SEQUENCE;
import static io.nats.client.support.NatsKeyValueUtil.*;
import static io.nats.client.support.Validator.*;

public class NatsKeyValue extends NatsFeatureBase implements KeyValue {

    private final String bucketName;
    private final String streamName;
    private final String streamSubject;
    private final String plainKeyPrefix;
    private final String publishKeyPrefix;

    NatsKeyValue(NatsConnection connection, String bucketName, KeyValueOptions kvo) throws IOException {
        super(connection, kvo);
        this.bucketName = Validator.validateKvBucketNameRequired(bucketName);
        streamName = toStreamName(bucketName);
        streamSubject = toStreamSubject(bucketName);
        plainKeyPrefix = toKeyPrefix(bucketName);
        if (kvo == null) {
            publishKeyPrefix = plainKeyPrefix;
        }
        else {
            publishKeyPrefix = kvo.getFeaturePrefix() == null ? plainKeyPrefix : kvo.getFeaturePrefix();
        }
    }

    String toPlainKeySubject(String key) {
        return plainKeyPrefix + key;
    }

    String toPublishKeySubject(String key) {
        return publishKeyPrefix + key;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getStoreName() {
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
        return new KeyValueEntry(_getLastMessage(streamName, toPlainKeySubject(key)));
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
        return _publish(toPublishKeySubject(key), data, h);
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
        visitSubject(toPlainKeySubject(">"), DeliverPolicy.LastPerSubject, true, false, m -> {
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
        visitSubject(toPlainKeySubject(key), DeliverPolicy.All, false, true, m -> list.add(new KeyValueEntry(m)));
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
            jsm.purgeStream(streamName, PurgeOptions.subject(toPlainKeySubject(key)));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KeyValueStatus getStatus() throws IOException, JetStreamApiException, InterruptedException {
        return new KeyValueStatus(jsm.getStreamInfo(streamName));
    }
}
