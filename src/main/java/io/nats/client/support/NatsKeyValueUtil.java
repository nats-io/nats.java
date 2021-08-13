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

import io.nats.client.api.KvOperation;
import io.nats.client.impl.Headers;

import static io.nats.client.support.NatsConstants.DOT;

public abstract class NatsKeyValueUtil {

    public static final String KV_STREAM_PREFIX = "KV_";
    public static final int KV_STREAM_PREFIX_LEN = KV_STREAM_PREFIX.length();
    public static final String KV_SUBJECT_PREFIX = "$KV.";
    public static final String KV_SUBJECT_SUFFIX = ".>";
    public static final String KV_OPERATION_HEADER_KEY = "KV-Operation";

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

    public static Headers addDeleteHeader(Headers h) {
        return h.put(KV_OPERATION_HEADER_KEY, KvOperation.DEL.name());
    }

    public static String getHeader(Headers h) {
        return h == null ? null : h.getFirst(KV_OPERATION_HEADER_KEY);
    }
}
