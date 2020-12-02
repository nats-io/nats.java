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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeoutException;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Subscription;

class NatsMessage implements Message {
    private String sid;
    private String subject;
    private String replyTo;
    private ByteBuffer data;
    private ByteBuffer protocolBytes;
    private NatsSubscription subscription;
    private Integer protocolLength = null;
    private MetaData jsMetaData = null;

    NatsMessage next; // for linked list

    // Acknowedgement protocol messages
    private static final byte[] AckAck = "+ACK".getBytes();
    private static final byte[] AckNak = "-NAK".getBytes();
    private static final byte[] AckProgress = "+WPI".getBytes();

    // special case
    private static final byte[] AckNextEmptyPayload = "+NXT {}".getBytes();
    private static final byte[] AckNext = "+NXT".getBytes();

    private static final byte[] AckTerm = "+TERM".getBytes();

    static final DateTimeFormatter rfc3339Formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX");

    // Create a message to publish
    NatsMessage(String subject, String replyTo, ByteBuffer data, boolean utf8mode) {
        this.subject = subject;
        this.replyTo = replyTo;
        this.data = data;
        Charset charset;

        // Calculate the length in bytes
        int size = (data != null) ? data.limit() : 0;
        int len = fastIntLength(size) + 4;
        if (replyTo != null) {
            if (utf8mode) {
                len += fastUtf8Length(replyTo) + 1;
            } else {
                len += replyTo.length() + 1;
            }
        }
        if (utf8mode) {
            len += fastUtf8Length(subject) + 1;
            charset = StandardCharsets.UTF_8;
        } else {
            len += subject.length() + 1;
            charset = StandardCharsets.US_ASCII;
        }
        this.protocolBytes = ByteBuffer.allocate(len);
        protocolBytes.put((byte) 'P').put((byte) 'U').put((byte) 'B').put((byte) ' ');
        protocolBytes.put(subject.getBytes(charset));
        protocolBytes.put((byte) ' ');

        if (replyTo != null) {
            protocolBytes.put(replyTo.getBytes(charset));
            protocolBytes.put((byte) ' ');
        }

        if (size > 0) {
            int base = protocolBytes.limit();
            for (int i = size; i > 0; i /= 10) {
                base--;
                protocolBytes.put(base, (byte) (i % 10 + (byte) '0'));
            }
        } else {
            protocolBytes.put((byte) '0');
        }
        protocolBytes.clear();
    }

    NatsMessage(String subject, String replyTo, byte[] data, boolean utf8mode) {
        this(subject, replyTo, ByteBuffer.wrap(data), utf8mode);
    }

    // Create a protocol only message to publish
    NatsMessage(CharBuffer protocol) {
        if (protocol.remaining() == 0) {
            this.protocolBytes = ByteBuffer.allocate(0);
        } else {
            protocol.mark();
            this.protocolBytes = ByteBuffer.allocate(fastUtf8Length(protocol));
            protocol.reset();
            StandardCharsets.UTF_8.newEncoder().encode(protocol, this.protocolBytes, true);
            protocolBytes.clear();
        }
    }

    // Create an incoming message for a subscriber
    // Doesn't check controlline size, since the server sent us the message
    NatsMessage(String sid, String subject, String replyTo, int protocolLength) {
        this.sid = sid;
        this.subject = subject;
        if (replyTo != null) {
            this.replyTo = replyTo;
        }
        this.protocolLength = protocolLength;
        this.data = null; // will set data and size after we read it
    }

    private static int fastUtf8Length(CharSequence cs) {
        int count = 0;
        for (int i = 0, len = cs.length(); i < len; i++) {
            char ch = cs.charAt(i);
            if (ch <= 0x7F) {
                count++;
            } else if (ch <= 0x7FF) {
                count += 2;
            } else if (Character.isHighSurrogate(ch)) {
                count += 4;
                ++i;
            } else {
                count += 3;
            }
        }
        return count;
    }

    private static int fastIntLength(int number) {
        if (number < 100000) {
            if (number < 100) {
                if (number < 10) {
                    return 1;
                } else {
                    return 2;
                }
            } else {
                if (number < 1000) {
                    return 3;
                } else {
                    if (number < 10000) {
                        return 4;
                    } else {
                        return 5;
                    }
                }
            }
        } else {
            if (number < 10000000) {
                if (number < 1000000) {
                    return 6;
                } else {
                    return 7;
                }
            } else {
                if (number < 100000000) {
                    return 8;
                } else {
                    if (number < 1000000000) {
                        return 9;
                    } else {
                        return 10;
                    }
                }
            }
        }
    }

    boolean isProtocol() {
        return this.subject == null;
    }

    // Will be null on an incoming message
    byte[] getProtocolBytes() {
        return this.protocolBytes.array();
    }

    int getControlLineLength() {
        return (this.protocolBytes != null) ? this.protocolBytes.limit() + 2 : -1;
    }

    long getSizeInBytes() {
        long sizeInBytes = 0;
        if (this.protocolBytes != null) {
            sizeInBytes += this.protocolBytes.limit();
        }
        if (this.protocolLength != null) {
            sizeInBytes += this.protocolLength;
        }
        if (data != null) {
            sizeInBytes += data.limit() + 4;// for 2x \r\n
        } else {
            sizeInBytes += 2;
        }
        return sizeInBytes;
    }

    public String getSID() {
        return this.sid;
    }

    // Only for incoming messages, with no protocol bytes
    void setData(byte[] data) {
        this.data = ByteBuffer.wrap(data);
    }

    void setSubscription(NatsSubscription sub) {
        this.subscription = sub;
    }

    NatsSubscription getNatsSubscription() {
        return this.subscription;
    }

    public Connection getConnection() {
        if (this.subscription == null) {
            return null;
        }

        return this.subscription.connection;
    }

    public String getSubject() {
        return this.subject;
    }

    public String getReplyTo() {
        return this.replyTo;
    }

    public byte[] getData() {
        return this.data.array();
    }

    public Subscription getSubscription() {
        return this.subscription;
    }

    private void jsAck(byte[] body, Duration d) throws InterruptedException, TimeoutException {
        if (this.replyTo == null || this.replyTo.isEmpty()) {
            throw new IllegalStateException("Message is not a jestream message");
        }

        Connection c = getConnection();
        if (c == null) {
            throw new IllegalStateException("Message is not bound to a connection");
        }

        if (d == Duration.ZERO) {
            c.publish(replyTo, body);
        } else {
            Message m = c.request(replyTo, body, d);
            if (m == null) {
                throw new TimeoutException("Timed out waiting for an ack confirmation");
            }
        }
    }

    @Override
    public void ack(Duration timeout) throws InterruptedException, TimeoutException {
        jsAck(AckAck, timeout);
    }

    @Override
    public void nak(Duration timeout)  throws InterruptedException, TimeoutException {
        jsAck(AckNak, timeout);
    }

    @Override
    public void ackProgress(Duration timeout) throws InterruptedException, TimeoutException {
        jsAck(AckProgress, timeout);
    }

    @Override
    public void ackNext() {
        Connection c = getConnection();
        if (c == null) {
            throw new IllegalStateException("Message is not bound to a connection");
        }
        
        c.publish(replyTo, subject, AckNext);
    }

    @Override
    public void ackNextRequest(ZonedDateTime expiry, long batch, boolean noWait) {

        if (batch < 0) {
            throw new IllegalArgumentException();
        }

        Connection c = getConnection();
        if (c == null) {
            throw new IllegalStateException("Message is not bound to a connection");
        }
        
        // minor optimization for the ack.
        byte[] payload;
        if (expiry == null && batch == 0 && !noWait) {
            payload = AckNextEmptyPayload;
        } else {
            StringBuilder sb = new StringBuilder("+ACKNXT {");
            if (expiry != null) {
                String s = rfc3339Formatter.format(expiry);
                sb.append("\"expires\" : \"" + s + "\",");
            }
            if (batch > 0) {
                sb.append("\"batch\" : " + batch + ",");
            }
            if (noWait) {
                sb.append("\"no_wait\" : true");
            }
            
            // remove potential trailing ','
            if (sb.codePointAt(sb.length()-1) == ',') {
               sb.setLength(sb.length()-1);
            }
            
            sb.append("}");
            payload = sb.toString().getBytes();
        }

        c.publish(replyTo, subject, payload);
    }

    @Override
    public Message ackAndFetch(Duration timeout) throws InterruptedException {
        if (this.replyTo == null || this.replyTo.isEmpty()) {
            throw new IllegalStateException("Message is not a jestream message");
        }

        Connection c = getConnection();
        if (c == null) {
            throw new IllegalStateException("Message is not bound to a connection");
        }

        Subscription s = c.subscribe(c.createInbox());
        s.unsubscribe(1);

        c.publish(replyTo, s.getSubject(), AckNext);

        return s.nextMessage(timeout);
    }

    @Override
    public void ackTerm(Duration timeout) throws InterruptedException, TimeoutException {
        jsAck(AckTerm, timeout);
    }

    public MetaData metaData() {
        if (this.jsMetaData == null) {
            this.jsMetaData = new NatsJetstreamMetaData();
        }
        return this.jsMetaData;
    }

    public class NatsJetstreamMetaData implements Message.MetaData {

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

        NatsJetstreamMetaData() {
            if (replyTo == null || replyTo.isEmpty()) {
                throwNotJSMsgException(replyTo);
            }

            String[] parts = replyTo.split("\\.");
            if (parts.length < 8 || parts.length > 9 || !"$JS".equals(parts[0]) || !"ACK".equals(parts[1])) {
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
}
