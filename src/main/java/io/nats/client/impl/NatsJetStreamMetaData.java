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
import java.util.HashMap;
import java.util.Map;

/**
 * Jetstream meta data about a message, when applicable.
 */
public class NatsJetStreamMetaData {

    private static final long NANO_FACTOR = 10_00_000_000;

    private final String prefix;
    private final String domain;
    private final String accountHash;
    private final String stream;
    private final String consumer;
    private final long delivered;
    private final long streamSeq;
    private final long consumerSeq;
    private final ZonedDateTime timestamp;
    private final long pending;
    private final String token;

    @Override
    public String toString() {
        return "NatsJetStreamMetaData{" +
                "prefix='" + prefix + '\'' +
                "domain='" + domain + '\'' +
                ", accountHash='" + accountHash + '\'' +
                ", stream='" + stream + '\'' +
                ", consumer='" + consumer + '\'' +
                ", delivered=" + delivered +
                ", streamSeq=" + streamSeq +
                ", consumerSeq=" + consumerSeq +
                ", timestamp=" + timestamp +
                ", pending=" + pending +
                ", token=" + token +
                '}';
    }

    /*
    v0 <prefix>.ACK.<stream name>.<consumer name>.<num delivered>.<stream sequence>.<consumer sequence>.<timestamp>
    v1 <prefix>.ACK.<stream name>.<consumer name>.<num delivered>.<stream sequence>.<consumer sequence>.<timestamp>.<num pending>
    v2 <prefix>.ACK.<domain>.<account hash>.<stream name>.<consumer name>.<num delivered>.<stream sequence>.<consumer sequence>.<timestamp>.<num pending>.<a token with a random value>
     */

    enum AckReplyToVer {
        V0(8, 2, false, false),
        V1(9, 2, true, false),
        V2(12, 4, true, true);

        final static Map<Integer, AckReplyToVer> byNumParts;

        final int parts;
        final int streamIndex;
        final boolean pending;
        final boolean domainHashToken;

        static {
            byNumParts = new HashMap<>();
            for (AckReplyToVer v : AckReplyToVer.values()) {
                byNumParts.put(v.parts, v);
            }
        }

        AckReplyToVer(int parts, int streamIndex, boolean pending, boolean domainHashToken) {
            this.parts = parts;
            this.streamIndex = streamIndex;
            this.pending = pending;
            this.domainHashToken = domainHashToken;
        }

        public static AckReplyToVer byNumParts(int parts) {
            return byNumParts.get(parts);
        }
    }

    public NatsJetStreamMetaData(NatsMessage natsMessage) {
        if (!natsMessage.isJetStream()) {
            throw new IllegalArgumentException(notAJetStreamMessage(natsMessage.getReplyTo()));
        }

        String[] parts = natsMessage.getReplyTo().split("\\.");
        AckReplyToVer ver = AckReplyToVer.byNumParts(parts.length);
        if (ver == null || !"ACK".equals(parts[1])) {
            throw new IllegalArgumentException(notAJetStreamMessage(natsMessage.getReplyTo()));
        }

        prefix = parts[0];
        // "ack" <= parts[1]
        domain = ver.domainHashToken ? parts[2] : null;
        accountHash = ver.domainHashToken ? parts[3] : null;
        stream = parts[ver.streamIndex];
        consumer = parts[ver.streamIndex + 1];
        delivered = Long.parseLong(parts[ver.streamIndex + 2]);
        streamSeq = Long.parseLong(parts[ver.streamIndex + 3]);
        consumerSeq = Long.parseLong(parts[ver.streamIndex + 4]);

        // not so clever way to separate nanos from seconds
        long tsi = Long.parseLong(parts[ver.streamIndex + 5]);
        long seconds = tsi / NANO_FACTOR;
        int nanos = (int) (tsi - ((tsi / NANO_FACTOR) * NANO_FACTOR));
        LocalDateTime ltd = LocalDateTime.ofEpochSecond(seconds, nanos, OffsetDateTime.now().getOffset());
        timestamp = ZonedDateTime.of(ltd, ZoneId.systemDefault()); // I think this is safe b/c the zone should match local

        pending = ver.pending ? Long.parseLong(parts[ver.streamIndex + 6]) : -1L;
        token = ver.domainHashToken ? parts[ver.streamIndex + 7] : null;
    }

    /**
     * Get the domain for the message. Might be null
     * @return the domain
     */
    public String getDomain() {
        return domain;
    }

    /**
     * Get the hash of the account. Might be null
     * @return the hash
     */
    public String getAccountHash() {
        return accountHash;
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

    /**
     * Get the ack token
     * @return the token
     */
    public String getToken() {
        return token;
    }

    private String notAJetStreamMessage(String reply) {
        return "Message is not a JetStream message.  ReplySubject: <" + reply + ">";
    }
}
