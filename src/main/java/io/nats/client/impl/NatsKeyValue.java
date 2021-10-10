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
import io.nats.client.api.KvEntry;
import io.nats.client.api.MessageInfo;
import io.nats.client.api.PublishAck;
import io.nats.client.support.Validator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static io.nats.client.api.MessageGetRequest.lastBySubjectBytes;
import static io.nats.client.support.NatsKeyValueUtil.*;

public class NatsKeyValue extends NatsJetStreamImplBase implements KeyValue {

    private final static Headers HEADERS_DELETE_INSTRUCTION;

    private final String bucket;
    private final String stream;
    private final JetStream js;

    static {
        HEADERS_DELETE_INSTRUCTION = addDeleteHeader(new Headers());
    }

    public NatsKeyValue(String bucket, NatsConnection connection, JetStreamOptions jsOptions) throws IOException {
        super(connection, jsOptions);
        this.bucket = Validator.validateBucketNameRequired(bucket);
        stream = streamName(this.bucket);
        js = new NatsJetStream(connection, jsOptions);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] getValue(String key) throws IOException, JetStreamApiException {
        KvEntry entry = getEntry(key);
        return entry == null ? null : entry.getData();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getStringValue(String key) throws IOException, JetStreamApiException {
        byte[] value = getValue(key);
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long getLongValue(String key) throws IOException, JetStreamApiException {
        byte[] value = getValue(key);
        return value == null ? null : Long.parseLong(new String(value, StandardCharsets.US_ASCII));
    }

    @Override
    public KvEntry getEntry(String key) throws IOException, JetStreamApiException {
        Validator.validateKeyRequired(key);
        String subj = String.format(JSAPI_MSG_GET, stream);
        Message resp = makeRequestResponseRequired(subj, lastBySubjectBytes(keySubject(bucket, key)), jso.getRequestTimeout());
        MessageInfo mi = new MessageInfo(resp);
        if (mi.hasError()) {
            if (mi.getApiErrorCode() == JS_NO_MESSAGE_FOUND_ERR) {
                // run of the mill key not found
                return null;
            }
            mi.throwOnHasError();
        }
        return new KvEntry(mi);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long put(String key, byte[] value) throws IOException, JetStreamApiException {
        Validator.validateKeyRequired(key);
        PublishAck pa = js.publish(NatsMessage.builder()
                .subject(keySubject(bucket, key))
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
    public long delete(String key) throws IOException, JetStreamApiException {
        Validator.validateKeyRequired(key);
        PublishAck pa = js.publish(NatsMessage.builder()
                .subject(keySubject(bucket, key))
                .headers(HEADERS_DELETE_INSTRUCTION)
                .build());
        return pa.getSeqno();
    }
}
