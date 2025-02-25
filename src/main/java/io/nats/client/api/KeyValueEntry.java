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
package io.nats.client.api;

import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.nats.client.support.NatsKeyValueUtil;

import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;

import static io.nats.client.support.NatsJetStreamConstants.MSG_SIZE_HDR;
import static io.nats.client.support.NatsKeyValueUtil.BucketAndKey;

/**
 * The KeyValueEntry represents a record in the Key Value history
 */
public class KeyValueEntry {

    private final BucketAndKey bucketAndKey;
    private final byte[] value;
    private final long dataLen;
    private final ZonedDateTime created;
    private final long revision;
    private final long delta;
    private final KeyValueOperation op;

    public KeyValueEntry(MessageInfo mi) {
        Headers h = mi.getHeaders();
        bucketAndKey = new BucketAndKey(mi.getSubject());
        value = extractValue(mi.getData());
        dataLen = calculateLength(value, h);
        created = mi.getTime();
        revision = mi.getSeq();
        delta = 0;
        op = NatsKeyValueUtil.getOperation(h);
    }

    public KeyValueEntry(Message m) {
        Headers h = m.getHeaders();
        bucketAndKey = new BucketAndKey(m.getSubject());
        value = extractValue(m.getData());
        dataLen = calculateLength(value, h);
        created = m.metaData().timestamp();
        revision = m.metaData().streamSequence();
        delta = m.metaData().pendingCount();
        op = NatsKeyValueUtil.getOperation(h);
    }

    public String getBucket() {
        return bucketAndKey.bucket;
    }

    public String getKey() {
        return bucketAndKey.key;
    }

    public byte[] getValue() {
        return value;
    }

    public String getValueAsString() {
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }

    public Long getValueAsLong() {
        return value == null ? null : Long.parseLong(new String(value, StandardCharsets.US_ASCII));
    }

    public long getDataLen() {
        return dataLen;
    }

    public ZonedDateTime getCreated() {
        return created;
    }

    public long getRevision() {
        return revision;
    }

    public long getDelta() {
        return delta;
    }

    public KeyValueOperation getOperation() {
        return op;
    }

    @Override
    public String toString() {
        return "KvEntry{" +
            "bucket='" + bucketAndKey.bucket + '\'' +
            ", key='" + bucketAndKey.key + '\'' +
            ", operation=" + op +
            ", revision=" + revision +
            ", delta=" + delta +
            ", dataLen=" + dataLen +
            ", created=" + created +
            '}';
    }

    private static byte[] extractValue(byte[] data) {
        return data == null || data.length == 0 ? null : data;
    }

    private static long calculateLength(byte[] value, Headers h) {
        if (value == null) {
            String hlen = h == null ? null : h.getFirst(MSG_SIZE_HDR);
            return hlen == null ? 0 : Long.parseLong(hlen);
        }
        return value.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KeyValueEntry that = (KeyValueEntry) o;

        return bucketAndKey.equals(that.bucketAndKey)
            && revision == that.revision;
    }

    @Override
    public int hashCode() {
        int result = bucketAndKey.hashCode();
        result = 31 * result + Long.hashCode(revision);
        return result;
    }
}
