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
import io.nats.client.support.DateTimeUtils;
import io.nats.client.support.Validator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static io.nats.client.support.NatsConstants.DOT;
import static io.nats.client.support.NatsConstants.GREATER_THAN;
import static io.nats.client.support.NatsJetStreamConstants.EXPECTED_LAST_SUB_SEQ_HDR;
import static io.nats.client.support.NatsJetStreamConstants.JS_WRONG_LAST_SEQUENCE;
import static io.nats.client.support.NatsKeyValueUtil.*;
import static io.nats.client.support.Validator.*;

public class NatsKeyValue extends NatsFeatureBase implements KeyValue {

    private final String bucketName;
    private final String streamSubject;
    private final String readPrefix;
    private final String writePrefix;

    NatsKeyValue(NatsConnection connection, String bucketName, KeyValueOptions kvo) throws IOException {
        super(connection, kvo);
        this.bucketName = Validator.validateBucketName(bucketName, true);
        streamName = toStreamName(bucketName);
        StreamInfo si;
        try {
             si = jsm.getStreamInfo(streamName);
        } catch (JetStreamApiException e) {
            // can't throw directly, that would be a breaking change
            throw new IOException(e);
        }

        streamSubject = toStreamSubject(bucketName);
        String readTemp = toKeyPrefix(bucketName);

        String writeTemp;
        Mirror m = si.getConfiguration().getMirror();
        if (m != null) {
            String bName = trimPrefix(m.getName());
            String mExtApi = m.getExternal() == null ? null : m.getExternal().getApi();
            if (mExtApi == null) {
                writeTemp = toKeyPrefix(bName);
            }
            else {
                readTemp = toKeyPrefix(bName);
                writeTemp = mExtApi + DOT + toKeyPrefix(bName);
            }
        }
        else if (kvo == null || kvo.getJetStreamOptions().isDefaultPrefix()) {
            writeTemp = readTemp;
        }
        else {
            writeTemp = kvo.getJetStreamOptions().getPrefix() + readTemp;
        }

        readPrefix = readTemp;
        writePrefix = writeTemp;
    }

    String readSubject(String key) {
        return readPrefix + key;
    }

    String writeSubject(String key) {
        return writePrefix + key;
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
    public KeyValueEntry get(String key) throws IOException, JetStreamApiException {
        return existingOnly(_get(validateNonWildcardKvKeyRequired(key)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KeyValueEntry get(String key, long revision) throws IOException, JetStreamApiException {
        return existingOnly(_get(validateNonWildcardKvKeyRequired(key), revision));
    }

    KeyValueEntry existingOnly(KeyValueEntry kve) {
        return kve == null || kve.getOperation() != KeyValueOperation.PUT ? null : kve;
    }

    KeyValueEntry _get(String key) throws IOException, JetStreamApiException {
        MessageInfo mi = _getLast(readSubject(key));
        return mi == null ? null : new KeyValueEntry(mi);
    }

    KeyValueEntry _get(String key, long revision) throws IOException, JetStreamApiException {
        MessageInfo mi = _getBySeq(revision);
        if (mi != null) {
            KeyValueEntry kve = new KeyValueEntry(mi);
            if (key.equals(kve.getKey())) {
                return kve;
            }
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long put(String key, byte[] value) throws IOException, JetStreamApiException {
        return _write(key, value, null).getSeqno();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long put(String key, String value) throws IOException, JetStreamApiException {
        return _write(key, value.getBytes(StandardCharsets.UTF_8), null).getSeqno();
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
                KeyValueEntry kve = _get(key);
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
        return _write(key, value, h).getSeqno();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long update(String key, String value, long expectedRevision) throws IOException, JetStreamApiException {
        return update(key, value.getBytes(StandardCharsets.UTF_8), expectedRevision);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(String key) throws IOException, JetStreamApiException {
        validateNonWildcardKvKeyRequired(key);
        _write(key, null, getDeleteHeaders());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(String key, long expectedRevision) throws IOException, JetStreamApiException {
        validateNonWildcardKvKeyRequired(key);
        Headers h = getDeleteHeaders().put(EXPECTED_LAST_SUB_SEQ_HDR, Long.toString(expectedRevision));
        _write(key, null, h).getSeqno();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void purge(String key) throws IOException, JetStreamApiException {
        _write(key, null, getPurgeHeaders());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void purge(String key, long expectedRevision) throws IOException, JetStreamApiException {
        Headers h = getPurgeHeaders().put(EXPECTED_LAST_SUB_SEQ_HDR, Long.toString(expectedRevision));
        _write(key, null, h);
    }

    private PublishAck _write(String key, byte[] data, Headers h) throws IOException, JetStreamApiException {
        validateNonWildcardKvKeyRequired(key);
        return js.publish(NatsMessage.builder().subject(writeSubject(key)).data(data).headers(h).build());
    }

    @Override
    public NatsKeyValueWatchSubscription watch(String key, KeyValueWatcher watcher, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException {
        validateKvKeyWildcardAllowedRequired(key);
        validateNotNull(watcher, "Watcher is required");
        return new NatsKeyValueWatchSubscription(this, Collections.singletonList(key), watcher, -1, watchOptions);
    }

    @Override
    public NatsKeyValueWatchSubscription watch(String key, KeyValueWatcher watcher, long fromRevision, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException {
        validateKvKeyWildcardAllowedRequired(key);
        validateNotNull(watcher, "Watcher is required");
        return new NatsKeyValueWatchSubscription(this, Collections.singletonList(key), watcher, fromRevision, watchOptions);
    }

    @Override
    public NatsKeyValueWatchSubscription watch(List<String> keys, KeyValueWatcher watcher, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException {
        validateKvKeysWildcardAllowedRequired(keys);
        validateNotNull(watcher, "Watcher is required");
        return new NatsKeyValueWatchSubscription(this, keys, watcher, -1, watchOptions);
    }

    @Override
    public NatsKeyValueWatchSubscription watch(List<String> keys, KeyValueWatcher watcher, long fromRevision, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException {
        validateKvKeysWildcardAllowedRequired(keys);
        validateNotNull(watcher, "Watcher is required");
        return new NatsKeyValueWatchSubscription(this, keys, watcher, fromRevision, watchOptions);
    }

    @Override
    public NatsKeyValueWatchSubscription watchAll(KeyValueWatcher watcher, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException {
        return new NatsKeyValueWatchSubscription(this, Collections.singletonList(GREATER_THAN), watcher, -1, watchOptions);
    }

    @Override
    public NatsKeyValueWatchSubscription watchAll(KeyValueWatcher watcher, long fromRevision, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException {
        return new NatsKeyValueWatchSubscription(this, Collections.singletonList(GREATER_THAN), watcher, fromRevision, watchOptions);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> keys() throws IOException, JetStreamApiException, InterruptedException {
        return _keys(Collections.singletonList(readSubject(GREATER_THAN)));
    }

    @Override
    public List<String> keys(String filter) throws IOException, JetStreamApiException, InterruptedException {
        return _keys(Collections.singletonList(readSubject(filter)));
    }

    @Override
    public List<String> keys(List<String> filters) throws IOException, JetStreamApiException, InterruptedException {
        List<String> readSubjectFilters = new ArrayList<>(filters.size());
        for (String f : filters) {
            readSubjectFilters.add(readSubject(f));
        }
        return _keys(readSubjectFilters);
    }

    private List<String> _keys(List<String> readSubjectFilters) throws IOException, JetStreamApiException, InterruptedException {
        List<String> list = new ArrayList<>();
        visitSubject(readSubjectFilters, DeliverPolicy.LastPerSubject, true, false, m -> {
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
    public LinkedBlockingQueue<KeyResult> consumeKeys() {
        return _consumeKeys(Collections.singletonList(readSubject(GREATER_THAN)));
    }

    @Override
    public LinkedBlockingQueue<KeyResult> consumeKeys(String filter) {
        return _consumeKeys(Collections.singletonList(readSubject(filter)));
    }

    @Override
    public LinkedBlockingQueue<KeyResult> consumeKeys(List<String> filters) {
        List<String> readSubjectFilters = new ArrayList<>(filters.size());
        for (String f : filters) {
            readSubjectFilters.add(readSubject(f));
        }
        return _consumeKeys(readSubjectFilters);
    }

    private LinkedBlockingQueue<KeyResult> _consumeKeys(List<String> readSubjectFilters) {
        LinkedBlockingQueue<KeyResult> q = new LinkedBlockingQueue<>();
        try {
            visitSubject(readSubjectFilters, DeliverPolicy.LastPerSubject, true, false, m -> {
                KeyValueOperation op = getOperation(m.getHeaders());
                if (op == KeyValueOperation.PUT) {
                    q.offer(new KeyResult(new BucketAndKey(m).key));
                }
            });
            q.offer(new KeyResult());
        }
        catch (IOException | JetStreamApiException e) {
            q.offer(new KeyResult(e));
        }
        catch (InterruptedException e) {
            q.offer(new KeyResult(e));
            Thread.currentThread().interrupt();
        }
        return q;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<KeyValueEntry> history(String key) throws IOException, JetStreamApiException, InterruptedException {
        validateNonWildcardKvKeyRequired(key);
        List<KeyValueEntry> list = new ArrayList<>();
        visitSubject(readSubject(key), DeliverPolicy.All, false, true, m -> list.add(new KeyValueEntry(m)));
        return list;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void purgeDeletes() throws IOException, JetStreamApiException, InterruptedException {
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
            jsm.purgeStream(streamName, PurgeOptions.subject(readSubject(key)));
        }

        for (String key : keep1List) {
            PurgeOptions po = PurgeOptions.builder()
                .subject(readSubject(key))
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
}
