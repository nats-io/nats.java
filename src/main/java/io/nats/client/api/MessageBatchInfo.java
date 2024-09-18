// Copyright 2024 The NATS Authors
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

import java.time.ZonedDateTime;

import static io.nats.client.support.NatsJetStreamConstants.*;

/**
 * The {@link MessageBatchInfo} class contains information about messages returned by a batch request.
 */
public class MessageBatchInfo {

    private final String subject;
    private final long seq;
    private final byte[] data;
    private final ZonedDateTime time;
    private final Headers headers;
    private final long lastSeq;
    private final long numPending;

    public MessageBatchInfo(Message msg) {
        Headers msgHeaders = msg.getHeaders();
        this.subject = msgHeaders.getLast(NATS_SUBJECT);
        this.data = msg.getData();
        this.seq = Long.parseLong(msgHeaders.getLast(NATS_SEQUENCE));
        this.time = DateTimeUtils.parseDateTime(msgHeaders.getLast(NATS_TIMESTAMP));
        long tmpLastSeq = Long.parseLong(msgHeaders.getLast(NATS_LAST_SEQUENCE));
        this.lastSeq = tmpLastSeq == 0 ? -1 : tmpLastSeq;

        // Num pending is +1 since it includes EOB message, correct that here.
        this.numPending = Long.parseLong(msgHeaders.getLast(NATS_NUM_PENDING)) - 1;

        // these are control headers, not real headers so don't give them to the user.
        headers = new Headers(msgHeaders, true, MESSAGE_INFO_HEADERS);
    }

    /**
     * Get the message subject
     *
     * @return the subject
     */
    public String getSubject() {
        return subject;
    }

    /**
     * Get the message sequence.
     * Can be used in a subsequent batch request if there are {@link #getNumPending()} messages.
     * In which case {@code getSeq() + 1} is used as the next minimum sequence number.
     *
     * @return the sequence number
     */
    public long getSeq() {
        return seq;
    }

    /**
     * Get the message data
     *
     * @return the data bytes
     */
    public byte[] getData() {
        return data;
    }

    /**
     * Get the time the message was received
     *
     * @return the time
     */
    public ZonedDateTime getTime() {
        return time;
    }

    /**
     * Get the headers
     *
     * @return the headers object or null if there were no headers
     */
    public Headers getHeaders() {
        return headers;
    }

    /**
     * Get the sequence number of the last message in the stream. Not always set.
     *
     * @return the last sequence or -1 if the value is not known.
     */
    public long getLastSeq() {
        return lastSeq;
    }

    /**
     * Amount of pending messages that can be requested with a subsequent batch request.
     *
     * @return number of pending messages
     */
    public long getNumPending() {
        return numPending;
    }
}
