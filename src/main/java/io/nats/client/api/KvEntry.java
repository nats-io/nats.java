// Copyright 2020 The NATS Authors
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

import java.time.ZonedDateTime;
import java.util.Arrays;

import static io.nats.client.support.NatsJetStreamConstants.KV_OPERATION_HEADER_KEY;

/**
 * The MessageInfo class contains information about a JetStream message.
 */
public class KvEntry extends ApiResponse<KvEntry> {

    private final String bucket;
    private final String key;
    private final long seq;
    private final byte[] data;
    private final ZonedDateTime created;
    private final KvOperation kvOperation;

    public KvEntry(MessageInfo mi, String bucket, String key) {
        this.bucket = bucket;
        this.key = key;
        seq = mi.getSeq();
        byte[] temp = mi.getData();
        data = temp == null || temp.length == 0 ? null : temp;
        created = mi.getTime();
        String operation = mi.getHeaders() == null ? null : mi.getHeaders().getFirst(KV_OPERATION_HEADER_KEY);
        kvOperation = KvOperation.getOrDefault(operation, KvOperation.PUT);
    }

    public KvEntry(Message m, String bucket, String key) {
        this.bucket = bucket;
        this.key = key;
        seq = m.metaData().streamSequence();
        byte[] temp = m.getData();
        data = temp == null || temp.length == 0 ? null : temp;
        created = m.metaData().timestamp();
        String operation = m.getHeaders() == null ? null : m.getHeaders().getFirst(KV_OPERATION_HEADER_KEY);
        kvOperation = KvOperation.getOrDefault(operation, KvOperation.PUT);
    }

    public String getBucket() {
        return bucket;
    }

    public String getKey() {
        return key;
    }

    public long getSeq() {
        return seq;
    }

    public byte[] getData() {
        return data;
    }

    public ZonedDateTime getCreated() {
        return created;
    }

    public KvOperation getKvOperation() {
        return kvOperation;
    }

    @Override
    public String toString() {
        return "KvEntry{" +
                "bucket='" + bucket + '\'' +
                ", key='" + key + '\'' +
                ", kvOperation=" + kvOperation +
                ", seq=" + seq +
                ", data=" + (data == null ? "null" : "[" + data.length + " bytes]") +
                ", created=" + created +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KvEntry kvEntry = (KvEntry) o;

        if (kvOperation != kvEntry.kvOperation) return false;
        if (seq != kvEntry.seq) return false;
        if (bucket != null ? !bucket.equals(kvEntry.bucket) : kvEntry.bucket != null) return false;
        if (key != null ? !key.equals(kvEntry.key) : kvEntry.key != null) return false;
        if (!Arrays.equals(data, kvEntry.data)) return false;
        long createdEs = created == null ? 0 : created.toEpochSecond();
        long thatEs = kvEntry.created == null ? 0 : kvEntry.created.toEpochSecond();
        return createdEs == thatEs;
    }

    @Override
    public int hashCode() {
        int result = bucket != null ? bucket.hashCode() : 0;
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (int) (seq ^ (seq >>> 32));
        result = 31 * result + Arrays.hashCode(data);
        result = 31 * result + (created != null ? Long.hashCode(created.toEpochSecond()) : 0);
        result = 31 * result + (kvOperation != null ? kvOperation.hashCode() : 0);
        return result;
    }
}
