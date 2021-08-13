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
import io.nats.client.support.JsonUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static io.nats.client.support.ApiConstants.LAST_BY_SUBJECT;
import static io.nats.client.support.NatsKeyValueUtil.*;

public class NatsKeyValue extends NatsJetStreamImplBase implements KeyValue {

    private final static Headers DELETE_HEADERS;

    private final JetStream js;
    private final String bucket;
    private final String stream;
    private final PushSubscribeOptions psoGet;

    static {
        DELETE_HEADERS = addDeleteHeader(new Headers());
    }

    public NatsKeyValue(String bucket, NatsConnection connection, JetStreamOptions jsOptions) throws IOException {
        super(connection, jsOptions);
        js = new NatsJetStream(connection, jsOptions);
        this.bucket = bucket;
        stream = streamName(bucket);
        psoGet = PushSubscribeOptions.builder()
                .stream(stream)
                .configuration(ConsumerConfiguration.builder().ackPolicy(AckPolicy.None).build())
                .build();
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
        String subj = String.format(JSAPI_MSG_GET, stream);
        Message resp = makeRequestResponseRequired(subj, JsonUtils.simpleMessageBody(LAST_BY_SUBJECT, keySubject(bucket, key)), jso.getRequestTimeout());
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
        PublishAck pa = js.publish(NatsMessage.builder()
                .subject(keySubject(bucket, key))
                .headers(DELETE_HEADERS)
                .build());
        return pa.getSeqno();
    }
}
