// Copyright 2022 The NATS Authors
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

import io.nats.client.impl.Headers;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static io.nats.client.support.NatsConstants.DOT;
import static io.nats.client.support.NatsJetStreamConstants.ROLLUP_HDR;
import static io.nats.client.support.NatsJetStreamConstants.ROLLUP_HDR_SUBJECT;

public abstract class NatsObjectStoreUtil {

    private NatsObjectStoreUtil() {} /* ensures cannot be constructed */

    public static final int DEFAULT_CHUNK_SIZE = 128 * 1024; // 128k
    public static final String OBJ_STREAM_PREFIX = "OBJ_";
    public static final int OBJ_STREAM_PREFIX_LEN = OBJ_STREAM_PREFIX.length();
    public static final String OBJ_SUBJECT_PREFIX = "$O.";
    public static final String OBJ_SUBJECT_SUFFIX = ".>";
    public static final String OBJ_META_PART = ".M";
    public static final String OBJ_CHUNK_PART = ".C";

    public final static Headers META_HEADERS;

    static {
        META_HEADERS = new Headers()
            .put(ROLLUP_HDR, ROLLUP_HDR_SUBJECT);
    }

    public static String extractBucketName(String streamName) {
        return streamName.substring(OBJ_STREAM_PREFIX_LEN);
    }

    public static String toStreamName(String bucketName) {
        return OBJ_STREAM_PREFIX + bucketName;
    }

    public static String toMetaStreamSubject(String bucketName) {
        return OBJ_SUBJECT_PREFIX + bucketName + OBJ_META_PART + OBJ_SUBJECT_SUFFIX;
    }

    public static String toChunkStreamSubject(String bucketName) {
        return OBJ_SUBJECT_PREFIX + bucketName + OBJ_CHUNK_PART + OBJ_SUBJECT_SUFFIX;
    }

    public static String toMetaPrefix(String bucketName) {
        return OBJ_SUBJECT_PREFIX + bucketName + OBJ_META_PART + DOT;
    }

    public static String toChunkPrefix(String bucketName) {
        return OBJ_SUBJECT_PREFIX + bucketName + OBJ_CHUNK_PART + DOT;
    }

    public static String encodeForSubject(String name) {
        return Base64.getEncoder().encodeToString(name.getBytes(StandardCharsets.UTF_8));
    }
}
