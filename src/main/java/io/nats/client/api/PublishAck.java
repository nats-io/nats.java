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

import io.nats.client.JetStreamApiException;
import io.nats.client.Message;
import io.nats.client.support.JsonUtils;

import java.io.IOException;

import static io.nats.client.support.ApiConstants.*;

/**
 * PublishAck objects represent a JetStream enabled server acknowledgment from a publish call.
 */
public class PublishAck extends ApiResponse<PublishAck> {

    private final String stream;
    private final long seq;
    private final boolean duplicate;

    public PublishAck(Message msg) throws IOException, JetStreamApiException {
        super(msg);
        throwOnHasError();
        stream = JsonUtils.readString(json, STREAM_RE, null);
        if (stream == null) {
            throw new IOException("Invalid JetStream ack.");
        }
        seq = JsonUtils.readLong(json, SEQ_RE, 0);
        if (seq == 0) {
            throw new IOException("Invalid JetStream ack.");
        }
        duplicate = JsonUtils.readBoolean(json, DUPLICATE_RE);
    }

    /**
     * Get the stream sequence number for the corresponding published message.
     * @return the sequence number for the stored message.
     */
    public long getSeqno() {
        return seq;
    }

    /**
     * Get the name of the stream a published message was stored in.
     * @return the the name of the stream.
     */
    public String getStream() {
        return stream;
    }

    /**
     * Gets if the server detected the published message was a duplicate.
     * @return true if the message is a duplicate, false otherwise.
     */
    public boolean isDuplicate() {
        return duplicate;
    }

    @Override
    public String toString() {
        return "PublishAck{" +
                "stream='" + stream + '\'' +
                ", seq=" + seq +
                ", duplicate=" + duplicate +
                "}";
    }
}
