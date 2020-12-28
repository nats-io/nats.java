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

import io.nats.client.MessageMetaData;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class NatsJetStreamMetaData implements MessageMetaData {

    private static final long NANO_FACTOR = 10_00_000_000;

    private final String stream;
    private final String consumer;
    private final long delivered;
    private final long streamSeq;
    private final long consumerSeq;
    private final ZonedDateTime timestamp;
    private long pending = -1;

    NatsJetStreamMetaData(NatsMessage natsMessage) {
        if (!natsMessage.isJetStream()) {
            throwNotAJetStreamMessage(natsMessage.replyTo);
        }

        String[] parts = natsMessage.replyTo.split("\\.");
        if (parts.length < 8 || parts.length > 9 || !"ACK".equals(parts[1])) {
            throwNotAJetStreamMessage(natsMessage.replyTo);
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
        timestamp = ZonedDateTime.of(ltd, ZoneId.systemDefault());

        if (parts.length == 9) {
            pending = Long.parseLong(parts[8]);
        }
    }

    @Override
    public String getStream() {
        return stream;
    }

    @Override
    public String getConsumer() {
        return consumer;
    }

    @Override
    public long deliveredCount() {
        return delivered;
    }

    @Override
    public long streamSequence() {
        return streamSeq;
    }

    @Override
    public long consumerSequence() {
        return consumerSeq;
    }

    @Override
    public long pendingCount() {
        return pending;
    }

    @Override
    public ZonedDateTime timestamp() {
        return timestamp;
    }

    private void throwNotAJetStreamMessage(String subject) {
        throw new IllegalArgumentException("Message is not a JetStream message.  ReplySubject: <" + subject + ">");
    }
}
