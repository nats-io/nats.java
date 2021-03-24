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
import io.nats.client.impl.Headers;
import io.nats.client.support.IncomingHeadersProcessor;
import io.nats.client.support.JsonUtils;

import java.time.ZonedDateTime;

import static io.nats.client.support.ApiConstants.*;

/**
 * The MessageInfo class contains information about a JetStream message.
 */
public class MessageInfo extends ApiResponse<MessageInfo> {

    private final String subject;
    private final long seq;
    private final byte[] data;
    private final ZonedDateTime time;
    private final Headers headers;

    public MessageInfo(Message msg) {
        super(msg);
        subject = JsonUtils.readString(json, SUBJECT_RE);
        data = JsonUtils.readBase64(json, DATA_RE);
        seq = JsonUtils.readLong(json, SEQ_RE, 0);
        time = JsonUtils.readDate(json, TIME_RE);
        byte[] hdrBytes = JsonUtils.readBase64(json, HDRS_RE);
        headers = hdrBytes == null ? null : new IncomingHeadersProcessor(hdrBytes).getHeaders();
    }

    /**
     * Get the message subject
     * @return the subject
     */
    public String getSubject() {
        return subject;
    }

    /**
     * Get the message sequence
     * @return the sequence number
     */
    public long getSeq() {
        return seq;
    }

    /**
     * Get the message data
     * @return the data bytes
     */
    public byte[] getData() {
        return data;
    }

    /**
     * Get the time the message was received
     * @return the time
     */
    public ZonedDateTime getTime() {
        return time;
    }

    /**
     * Get the headers
     * @return the headers object or null if there were no headers
     */
    public Headers getHeaders() {
        return headers;
    }

    @Override
    public String toString() {
        return "MessageInfo{" +
                "subject='" + subject + '\'' +
                ", seq=" + seq +
                ", data='" + data + '\'' +
                ", time=" + time +
                ", headers=" + headers +
                '}';
    }
}
