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
import io.nats.client.JetStreamOptions;
import io.nats.client.Message;
import io.nats.client.api.*;
import io.nats.client.support.JsonUtils;
import io.nats.client.support.NatsJetStreamConstants;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static io.nats.client.support.ApiConstants.SUBJECT;
import static io.nats.client.support.NatsJetStreamClientError.JsConsumerCreate290NotAvailable;

class NatsJetStreamImplBase implements NatsJetStreamConstants {

    // currently the only thing we care about caching is the allowDirect setting
    static class CachedStreamInfo {
        public final boolean allowDirect;

        public CachedStreamInfo(StreamInfo si) {
            allowDirect = si.getConfiguration().getAllowDirect();
        }
    }

    private static final ConcurrentHashMap<String, CachedStreamInfo> CACHED_STREAM_INFO_MAP = new ConcurrentHashMap<>();

    final NatsConnection conn;
    final JetStreamOptions jso;
    final boolean consumerCreate290Available;

    // ----------------------------------------------------------------------------------------------------
    // Create / Init
    // ----------------------------------------------------------------------------------------------------
    NatsJetStreamImplBase(NatsConnection connection, JetStreamOptions jsOptions) throws IOException {
        conn = connection;
        jso = JetStreamOptions.builder(jsOptions).build(); // builder handles null
        consumerCreate290Available = conn.getInfo().isSameOrNewerThanVersion("2.9.0") && !jso.isOptOut290ConsumerCreate();
    }

    // ----------------------------------------------------------------------------------------------------
    // Management that is also needed by regular context
    // ----------------------------------------------------------------------------------------------------
    ConsumerInfo _getConsumerInfo(String streamName, String consumer) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_CONSUMER_INFO, streamName, consumer);
        Message resp = makeRequestResponseRequired(subj, null, jso.getRequestTimeout());
        return new ConsumerInfo(resp).throwOnHasError();
    }

    ConsumerInfo _createConsumer(String streamName, ConsumerConfiguration config) throws IOException, JetStreamApiException {
        String name = config.getName();
        if (name != null && !consumerCreate290Available) {
            throw JsConsumerCreate290NotAvailable.instance();
        }

        String durable = config.getDurable();

        String consumerName = name == null ? durable : name;

        String subj;
        if (consumerName == null) { // just use old template
            subj = String.format(JSAPI_CONSUMER_CREATE, streamName);
        }
        else if (consumerCreate290Available) {
            String fs = config.getFilterSubject();
            if (fs == null || fs.equals(">")) {
                subj = String.format(JSAPI_CONSUMER_CREATE_V290, streamName, consumerName);
            }
            else {
                subj = String.format(JSAPI_CONSUMER_CREATE_V290_W_FILTER, streamName, consumerName, fs);
            }
        }
        else { // server is old and consumerName must be durable since name was checked for JsConsumerCreate290NotAvailable
            subj = String.format(JSAPI_DURABLE_CREATE, streamName, durable);
        }

        ConsumerCreateRequest ccr = new ConsumerCreateRequest(streamName, config);
        Message resp = makeRequestResponseRequired(subj, ccr.serialize(), conn.getOptions().getConnectionTimeout());
        return new ConsumerInfo(resp).throwOnHasError();
    }

    void _createConsumerUnsubscribeOnException(String stream, ConsumerConfiguration cc, NatsJetStreamSubscription sub) throws IOException, JetStreamApiException {
        try {
            ConsumerInfo ci = _createConsumer(stream, cc);
            sub.setConsumerName(ci.getName());
        }
        catch (IOException | JetStreamApiException e) {
            // create consumer can fail, unsubscribe and then throw the exception to the user
            if (sub.getDispatcher() == null) {
                sub.unsubscribe();
            }
            else {
                sub.getDispatcher().unsubscribe(sub);
            }
            throw e;
        }
    }

    StreamInfo _getStreamInfo(String streamName, StreamInfoOptions options) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_STREAM_INFO, streamName);
        byte[] payload = options == null ? null : options.serialize();
        Message resp = makeRequestResponseRequired(subj, payload, jso.getRequestTimeout());
        return createAndCacheStreamInfoThrowOnError(streamName, resp);
    }

    StreamInfo createAndCacheStreamInfoThrowOnError(String streamName, Message resp) throws JetStreamApiException {
        return cacheStreamInfo(streamName, new StreamInfo(resp).throwOnHasError());
    }

    StreamInfo cacheStreamInfo(String streamName, StreamInfo si) {
        CACHED_STREAM_INFO_MAP.put(streamName, new CachedStreamInfo(si));
        return si;
    }

    List<StreamInfo> cacheStreamInfo(List<StreamInfo> list) {
        list.forEach(si -> CACHED_STREAM_INFO_MAP.put(si.getConfiguration().getName(), new CachedStreamInfo(si)));
        return list;
    }

    List<String> _getStreamNamesBySubjectFilter(String subjectFilter) throws IOException, JetStreamApiException {
        byte[] body = JsonUtils.simpleMessageBody(SUBJECT, subjectFilter);
        StreamNamesReader snr = new StreamNamesReader();
        Message resp = makeRequestResponseRequired(JSAPI_STREAM_NAMES, body, jso.getRequestTimeout());
        snr.process(resp);
        return snr.getStrings();
    }

    // ----------------------------------------------------------------------------------------------------
    // Request Utils
    // ----------------------------------------------------------------------------------------------------
    Message makeRequestResponseRequired(String subject, byte[] bytes, Duration timeout) throws IOException {
        try {
            return responseRequired(conn.request(prependPrefix(subject), bytes, timeout));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    Message makeInternalRequestResponseRequired(String subject, Headers headers, byte[] data, boolean utf8mode, Duration timeout, boolean cancelOn503) throws IOException {
        try {
            return responseRequired(conn.requestInternal(subject, headers, data, utf8mode, timeout, cancelOn503));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    Message responseRequired(Message respMessage) throws IOException {
        if (respMessage == null) {
            throw new IOException("Timeout or no response waiting for NATS JetStream server");
        }
        return respMessage;
    }

    String prependPrefix(String subject) {
        return jso.getPrefix() + subject;
    }

    CachedStreamInfo getCachedStreamInfo(String streamName) throws IOException, JetStreamApiException {
        CachedStreamInfo csi = CACHED_STREAM_INFO_MAP.get(streamName);
        if (csi != null) {
            return csi;
        }
        _getStreamInfo(streamName, null);
        return CACHED_STREAM_INFO_MAP.get(streamName);
    }
}
