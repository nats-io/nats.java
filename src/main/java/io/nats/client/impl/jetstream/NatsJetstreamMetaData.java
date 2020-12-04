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

package io.nats.client.impl.jetstream;

import io.nats.client.impl.NatsMessage;
import io.nats.client.jetstream.MetaData;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class NatsJetstreamMetaData implements MetaData {

    private final NatsMessage natsMessage;
    private String stream;
    private String consumer;
    private long delivered;
    private long streamSeq;
    private long consumerSeq;
    private ZonedDateTime timestamp;
    private long pending = -1;

    private void throwNotJSMsgException(String subject) {
        throw new IllegalArgumentException("Message is not a jetstream message.  ReplySubject: <" + subject + ">");
    }

    public NatsJetstreamMetaData(NatsMessage natsMessage, String replyTo) {
        this.natsMessage = natsMessage;
        if (!natsMessage.isJetStream()) {
            throwNotJSMsgException(replyTo);
        }

        String[] parts = replyTo.split("\\.");
        if (parts.length < 8 || parts.length > 9 || !"ACK".equals(parts[1])) {
            throwNotJSMsgException(replyTo);
        }

        stream = parts[2];
        consumer = parts[3];
        delivered = Long.parseLong(parts[4]);
        streamSeq = Long.parseLong(parts[5]);
        consumerSeq = Long.parseLong(parts[6]);

        // not so clever way to seperate nanos from seconds
        long tsi = Long.parseLong(parts[7]);
        long seconds = tsi / 1000000000;
        int nanos = (int) (tsi - ((tsi / 1000000000) * 1000000000));
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
}
