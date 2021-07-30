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

import io.nats.client.support.JsonUtils;
import io.nats.client.support.Ulong;

import java.time.ZonedDateTime;

import static io.nats.client.support.ApiConstants.*;

public class StreamState {
    private final Ulong msgs;
    private final Ulong bytes;
    private final Ulong firstSeq;
    private final Ulong lastSeq;
    private final long consumerCount;
    private final ZonedDateTime firstTime;
    private final ZonedDateTime lastTime;

    StreamState(String json) {
        msgs = JsonUtils.readUlong(json, MESSAGES_RE, Ulong.ZERO);
        bytes = JsonUtils.readUlong(json, BYTES_RE, Ulong.ZERO);
        firstSeq = JsonUtils.readUlong(json, FIRST_SEQ_RE, Ulong.ZERO);
        lastSeq = JsonUtils.readUlong(json, LAST_SEQ_RE, Ulong.ZERO);
        consumerCount = JsonUtils.readLong(json, CONSUMER_COUNT_RE, 0);
        firstTime = JsonUtils.readDate(json, FIRST_TS_RE);
        lastTime = JsonUtils.readDate(json, LAST_TS_RE);
    }
    /**
     * Gets the message count of the stream.
     *
     * @return the message count
     */
    public Ulong getMessages() {
        return msgs;
    }

    /**
     * Gets the byte count of the stream.
     *
     * @return the byte count
     */
    public Ulong getBytes() {
        return bytes;
    }

    /**
     * Gets the first sequence number of the stream.
     *
     * @return a sequence number
     */
    public Ulong getFirstSequenceNum() {
        return firstSeq;
    }

    /**
     * Gets the last sequence of a message in the stream
     *
     * @return a sequence number
     */
    public Ulong getLastSequenceNum() {
        return lastSeq;
    }

    /**
     * Gets the message count of the stream.
     *
     * @return the message count
     */
    @Deprecated
    public long getMsgCount() {
        return msgs.value().longValueExact();
    }

    /**
     * Gets the byte count of the stream.
     *
     * @return the byte count
     */
    @Deprecated
    public long getByteCount() {
        return bytes.value().longValueExact();
    }

    /**
     * Gets the first sequence number of the stream.
     *
     * @return a sequence number
     */
    @Deprecated
    public long getFirstSequence() {
        return firstSeq.value().longValueExact();
    }

    /**
     * Gets the last sequence of a message in the stream
     *
     * @return a sequence number
     */
    @Deprecated
    public long getLastSequence() {
        return lastSeq.value().longValueExact();
    }

    /**
     * Gets the time stamp of the first message in the stream
     *
     * @return the first time
     */
    public ZonedDateTime getFirstTime() {
        return firstTime;
    }

    /**
     * Gets the time stamp of the last message in the stream
     *
     * @return the first time
     */
    public ZonedDateTime getLastTime() {
        return lastTime;
    }

    /**
     * Gets the number of consumers attached to the stream.
     *
     * @return the consumer count
     */
    public long getConsumerCount() {
        return consumerCount;
    }

    @Override
    public String toString() {
        return "StreamState{" +
                "msgs=" + msgs +
                ", bytes=" + bytes +
                ", firstSeq=" + firstSeq +
                ", lastSeq=" + lastSeq +
                ", consumerCount=" + consumerCount +
                ", firstTime=" + firstTime +
                ", lastTime=" + lastTime +
                '}';
    }
}
