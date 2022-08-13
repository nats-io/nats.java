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
import io.nats.client.support.DateTimeUtils;
import io.nats.client.support.IncomingHeadersProcessor;
import io.nats.client.support.JsonUtils;

import java.time.ZonedDateTime;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.NatsJetStreamConstants.*;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * The MessageInfo class contains information about a JetStream message.
 */
public class MessageInfo extends ApiResponse<MessageInfo> {

    private final String subject;
    private final long seq;
    private final byte[] data;
    private final ZonedDateTime time;
    private final Headers headers;
    private final String stream;
    private final long lastSeq;

    /**
     * @deprecated This signature was public for unit testing but is no longer used.
     */
    @Deprecated
    public MessageInfo(Message msg) {
        this(msg, null, false);
    }

    public MessageInfo(Message msg, String streamName, boolean fromDirect) {
        super(fromDirect ? null : new String(msg.getData(), UTF_8));

        if (fromDirect) {
            this.headers = msg.getHeaders();
            this.subject = headers.getFirst(NATS_SUBJECT);
            this.data = msg.getData();
            seq = Long.parseLong(headers.getFirst(NATS_SEQUENCE));
            time = DateTimeUtils.parseDateTime(headers.getFirst(NATS_TIMESTAMP));
            stream = headers.getFirst(NATS_STREAM);
            String temp = headers.getFirst(NATS_LAST_SEQUENCE);
            if (temp == null) {
                lastSeq = -1;
            }
            else {
                lastSeq = JsonUtils.safeParseLong(temp, -1);
            }
            // these are control headers, not real headers so don't give them to the user.
            headers.remove(NATS_SUBJECT, NATS_SEQUENCE, NATS_TIMESTAMP, NATS_STREAM, NATS_LAST_SEQUENCE);
        }
        else if (hasError()) {
            subject = null;
            data = null;
            seq = -1;
            time = null;
            headers = null;
            stream = null;
            lastSeq = -1;
        }
        else {
            subject = JsonUtils.readString(json, SUBJECT_RE);
            data = JsonUtils.readBase64(json, DATA_RE);
            seq = JsonUtils.readLong(json, SEQ_RE, 0);
            time = JsonUtils.readDate(json, TIME_RE);
            byte[] hdrBytes = JsonUtils.readBase64(json, HDRS_RE);
            headers = hdrBytes == null ? null : new IncomingHeadersProcessor(hdrBytes).getHeaders();
            stream = streamName;
            lastSeq = -1;
        }
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

    public String getStream() {
        return stream;
    }

    public long getLastSeq() {
        return lastSeq;
    }

    @Override
    public String toString() {
        return "MessageInfo{" +
            "subject='" + subject + '\'' +
            ", seq=" + seq +
            ", data=" + (data == null ? "null" :  '\'' + new String(data, UTF_8) + '\'') +
            ", time=" + time +
            (stream == null ? "" : ", stream=" + stream) +
            (lastSeq > 0 ? "" : ", lastSeq=" + lastSeq) +
            ", headers=" + headers +
            '}';
    }
}
