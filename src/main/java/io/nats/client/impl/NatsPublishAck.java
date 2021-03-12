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
package io.nats.client.impl;

import io.nats.client.Message;
import io.nats.client.PublishAck;

import java.io.IOException;

import static io.nats.client.support.ApiConstants.*;

public class NatsPublishAck extends JetStreamApiResponse<NatsPublishAck> implements PublishAck {

    private final String stream;
    private final long seq;
    private final boolean duplicate;

    public NatsPublishAck(Message msg) throws IOException, JetStreamApiException {
        super(msg);
        throwOnHasError();
        stream = JsonUtils.readString(json, STREAM_RE, null);
        if (stream == null || stream.length() == 0) {
            throw new IOException("Invalid JetStream ack.");
        }
        seq = JsonUtils.readLong(json, SEQ_RE, 0);
        if (seq == 0) {
            throw new IOException("Invalid JetStream ack.");
        }
        duplicate = JsonUtils.readBoolean(json, DUPLICATE_RE);
    }

    @Override
    public long getSeqno() {
        return seq;
    }

    @Override
    public String getStream() {
        return stream;
    }

    @Override
    public boolean isDuplicate() {
        return duplicate;
    }

    @Override
    public String toString() {
        return "NatsPublishAck{" +
                "stream='" + stream + '\'' +
                ", seq=" + seq +
                ", duplicate=" + duplicate +
                "}";
    }
}
