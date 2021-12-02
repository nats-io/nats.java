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

package io.nats.client.support;

import io.nats.client.Message;
import io.nats.client.api.KeyValueOperation;
import io.nats.client.impl.Headers;

import static io.nats.client.support.NatsConstants.DOT;
import static io.nats.client.support.NatsJetStreamConstants.ROLLUP_HDR;
import static io.nats.client.support.NatsJetStreamConstants.ROLLUP_HDR_SUBJECT;

public abstract class NatsKeyValueUtil {

    private NatsKeyValueUtil() {} /* ensures cannot be constructed */

    public static final String KV_STREAM_PREFIX = "KV_";
    public static final int KV_STREAM_PREFIX_LEN = KV_STREAM_PREFIX.length();
    public static final String KV_SUBJECT_PREFIX = "$KV.";
    public static final String KV_SUBJECT_SUFFIX = ".>";
    public static final String KV_OPERATION_HEADER_KEY = "KV-Operation";

    public final static Headers DELETE_HEADERS;
    public final static Headers PURGE_HEADERS;

    static {
        DELETE_HEADERS = new Headers()
            .put(KV_OPERATION_HEADER_KEY, KeyValueOperation.DEL.name());
        PURGE_HEADERS = new Headers()
            .put(KV_OPERATION_HEADER_KEY, KeyValueOperation.PURGE.name())
            .put(ROLLUP_HDR, ROLLUP_HDR_SUBJECT);
    }

    public static String streamName(String bucketName) {
        return KV_STREAM_PREFIX + bucketName;
    }

    public static String extractBucketName(String streamName) {
        return streamName.substring(KV_STREAM_PREFIX_LEN);
    }

    public static String streamSubject(String bucketName) {
        return KV_SUBJECT_PREFIX + bucketName + KV_SUBJECT_SUFFIX;
    }

    public static String keySubject(String bucketName, String key) {
        return KV_SUBJECT_PREFIX + bucketName + DOT + key;
    }

    public static String getOperationHeader(Headers h) {
        return h == null ? null : h.getFirst(KV_OPERATION_HEADER_KEY);
    }

    public static KeyValueOperation getOperation(Headers h, KeyValueOperation dflt) {
        return KeyValueOperation.getOrDefault(getOperationHeader(h), dflt);
    }

    public static class BucketAndKey {
        public final String bucket;
        public final String key;

        public BucketAndKey(Message m) {
            this(m.getSubject());
        }

        public BucketAndKey(String subject) {
            String[] split = subject.split("\\Q.\\E");
            bucket = split[1];
            key = split[2];
        }
    }
}
