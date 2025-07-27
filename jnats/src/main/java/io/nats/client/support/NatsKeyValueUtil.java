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
import io.nats.client.MessageTtl;
import io.nats.client.PublishOptions;
import io.nats.client.api.KeyValueOperation;
import io.nats.client.impl.Headers;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import static io.nats.client.support.NatsConstants.DOT;
import static io.nats.client.support.NatsJetStreamConstants.*;

public abstract class NatsKeyValueUtil {

    private NatsKeyValueUtil() {} /* ensures cannot be constructed */

    public static final String KV_STREAM_PREFIX = "KV_";
    public static final int KV_STREAM_PREFIX_LEN = KV_STREAM_PREFIX.length();
    public static final String KV_SUBJECT_PREFIX = "$KV.";
    public static final String KV_SUBJECT_SUFFIX = ".>";
    public static final String KV_OPERATION_HEADER_KEY = "KV-Operation";

    @NonNull
    public static String extractBucketName(String streamName) {
        return streamName.substring(KV_STREAM_PREFIX_LEN);
    }

    @NonNull
    public static String toStreamName(String bucketName) {
        return KV_STREAM_PREFIX + bucketName;
    }

    @NonNull
    public static String toStreamSubject(String bucketName) {
        return KV_SUBJECT_PREFIX + bucketName + KV_SUBJECT_SUFFIX;
    }

    @NonNull
    public static String toKeyPrefix(String bucketName) {
        return KV_SUBJECT_PREFIX + bucketName + DOT;
    }

    public static boolean hasPrefix(String bucketName) {
        return bucketName.startsWith(KV_STREAM_PREFIX);
    }

    @NonNull
    public static String trimPrefix(String bucketName) {
        if (bucketName.startsWith(KV_STREAM_PREFIX)) {
            return bucketName.substring(KV_STREAM_PREFIX.length());
        }
        return bucketName;
    }

    @Nullable
    public static String getOperationHeader(Headers h) {
        return h == null ? null : h.getFirst(KV_OPERATION_HEADER_KEY);
    }

    @Nullable
    public static String getNatsMarkerReasonHeader(Headers h) {
        return h == null ? null : h.getFirst(NATS_MARKER_REASON_HDR);
    }

    @NonNull
    public static KeyValueOperation getOperation(Headers h) {
        KeyValueOperation kvo = null;
        String hs = getOperationHeader(h);
        if (hs != null) {
            kvo = KeyValueOperation.instance(hs);
        }
        if (kvo == null) {
            hs = getNatsMarkerReasonHeader(h);
            if (hs != null) {
                kvo = KeyValueOperation.instanceByMarkerReason(hs);
            }
        }
        return kvo == null ? KeyValueOperation.PUT : kvo;
    }

    @NonNull
    public static Headers getDeleteHeaders() {
        return new Headers()
            .put(KV_OPERATION_HEADER_KEY, KeyValueOperation.DELETE.getHeaderValue());
    }

    @NonNull
    public static Headers getPurgeHeaders() {
        return new Headers()
            .put(KV_OPERATION_HEADER_KEY, KeyValueOperation.PURGE.getHeaderValue())
            .put(ROLLUP_HDR, ROLLUP_HDR_SUBJECT);
    }

    @Nullable
    public static PublishOptions getPublishOptions(long expectedRevision, MessageTtl messageTtl) {
        boolean returnNull = true;
        PublishOptions.Builder b = PublishOptions.builder();
        if (expectedRevision > -1) {
            returnNull = false;
            b.expectedLastSubjectSequence(expectedRevision);
        }
        if (messageTtl != null) {
            returnNull = false;
            b.messageTtl(messageTtl);
        }
        return returnNull ? null : b.build();
    }

    public static class BucketAndKey {
        public final String bucket;
        public final String key;

        public BucketAndKey(Message m) {
            this(m.getSubject());
        }

        public BucketAndKey(String subject) {
            String[] split = subject.split("\\Q.\\E", 3);
            bucket = split[1];
            key = split[2];
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            BucketAndKey that = (BucketAndKey) o;

            if (!bucket.equals(that.bucket)) return false;
            return key.equals(that.key);
        }

        @Override
        public int hashCode() {
            int result = bucket.hashCode();
            result = 31 * result + key.hashCode();
            return result;
        }
    }
}
