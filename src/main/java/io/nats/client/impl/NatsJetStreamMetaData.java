// Copyright 2015-2018 The NATS Authors
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

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Jetstream meta data about a message, when applicable.
 */
public class NatsJetStreamMetaData {

    private static final long NANO_FACTOR = 10_00_000_000;

    private final String stream;
    private final String consumer;
    private final long delivered;
    private final long streamSeq;
    private final long consumerSeq;
    private final ZonedDateTime timestamp;
    private long pending = -1;

    @Override
    public String toString() {
        return "NatsJetStreamMetaData{" +
                "stream='" + stream + '\'' +
                ", consumer='" + consumer + '\'' +
                ", delivered=" + delivered +
                ", streamSeq=" + streamSeq +
                ", consumerSeq=" + consumerSeq +
                ", timestamp=" + timestamp +
                ", pending=" + pending +
                '}';
    }

    public NatsJetStreamMetaData(NatsMessage natsMessage) {
        if (!natsMessage.isJetStream()) {
            throwNotAJetStreamMessage(natsMessage.getReplyTo());
        }

        String[] parts = natsMessage.getReplyTo().split("\\.");
        if (parts.length < 8 || parts.length > 9 || !"ACK".equals(parts[1])) {
            throwNotAJetStreamMessage(natsMessage.getReplyTo());
        }

        stream = parts[2];
        consumer = parts[3];
        delivered = Long.parseLong(parts[4]);
        streamSeq = Long.parseLong(parts[5]);
        consumerSeq = Long.parseLong(parts[6]);

        // not so clever way to separate nanos from seconds
        long tsi = Long.parseLong(parts[7]);
        long seconds = tsi / NANO_FACTOR;
        int nanos = (int) (tsi - ((tsi / NANO_FACTOR) * NANO_FACTOR));
        LocalDateTime ltd = LocalDateTime.ofEpochSecond(seconds, nanos, OffsetDateTime.now().getOffset());
        timestamp = ZonedDateTime.of(ltd, ZoneId.systemDefault()); // I think this is safe b/c the zone should match local

        if (parts.length == 9) {
            pending = Long.parseLong(parts[8]);
        }
    }

    /**
     * Gets the stream the message is from.
     *
     * @return the stream.
     */
    public String getStream() {
        return stream;
    }

    /**
     * Gets the consumer that generated this message.
     *
     * @return the consumer.
     */
    public String getConsumer() {
        return consumer;
    }

    /**
     * Gets the number of times this message has been delivered.
     *
     * @return delivered count.
     */
    public long deliveredCount() {
        return delivered;
    }

    /**
     * Gets the stream sequence number of the message.
     *
     * @return sequence number
     */
    public long streamSequence() {
        return streamSeq;
    }

    /**
     * Gets consumer sequence number of this message.
     *
     * @return sequence number
     */
    public long consumerSequence() {
        return consumerSeq;
    }

    /**
     * Gets the pending count of the consumer.
     *
     * @return pending count
     */
    public long pendingCount() {
        return pending;
    }

    /**
     * Gets the timestamp of the message.
     *
     * @return the timestamp
     */
    public ZonedDateTime timestamp() {
        return timestamp;
    }

    private void throwNotAJetStreamMessage(String subject) {
        throw new IllegalArgumentException("Message is not a JetStream message.  ReplySubject: <" + subject + ">");
    }
}
