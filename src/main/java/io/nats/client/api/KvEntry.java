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

import java.time.ZonedDateTime;

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
        data = mi.getData();
        created = mi.getTime();
        String operation = mi.getHeaders() == null ? null : mi.getHeaders().getFirst(KV_OPERATION_HEADER_KEY);
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
}
