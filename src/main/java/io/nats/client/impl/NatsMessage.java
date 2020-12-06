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

import java.nio.charset.Charset;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Subscription;
import io.nats.client.support.IncomingHeadersProcessor;
import io.nats.client.support.Status;

import java.nio.charset.Charset;

import static io.nats.client.support.NatsConstants.*;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

public class NatsMessage implements Message {

    public enum Kind {REGULAR, PROTOCOL, INCOMING}

    // Kind.REGULAR : just these fields
    private String subject;
    private String replyTo;
    private byte[] data;
    private boolean utf8mode;
    private Headers headers;
    private Status status;

    // Kind.INCOMING : subject, replyTo, data and these fields
    private String sid;
    private int protocolLineLength;

    // Kind.PROTOCOL : just this field
    private byte[] protocolBytes;

    // housekeeping
    private final Kind kind;
    private int sizeInBytes = -1;
    private int hdrLen = 0;
    private int dataLen = 0;
    private int totLen = 0;

    private NatsSubscription subscription;
    private Integer protocolLength = null;
    private MetaData jsMetaData = null;

    NatsMessage next; // for linked list

    // Acknowedgement protocol messages
    private static final byte[] AckAck = "+ACK".getBytes();
    private static final byte[] AckNak = "-NAK".getBytes();
    private static final byte[] AckProgress = "+WPI".getBytes();

    // special case
    private static final byte[] AckNext = "+NXT".getBytes();
    private static final byte[] AckTerm = "+TERM".getBytes();
    private static final byte[] AckNextOne = "+NXT {\"batch\":1}".getBytes();

    static final DateTimeFormatter rfc3339Formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX");

    public NatsMessage(String subject, String replyTo, byte[] data, boolean utf8mode) {
        this(subject, replyTo, null, data, utf8mode);
    }

    public NatsMessage(Message message) {
        this(message.getSubject(), message.getReplyTo(),
            message.hasHeaders() ? message.getHeaders() : null,
            message.getData(), message.isUtf8mode());
    }

    // Create a message to publish
    public NatsMessage(String subject, String replyTo, Headers headers, byte[] data, boolean utf8mode) {

        if (subject == null || subject.length() == 0) {
            throw new IllegalArgumentException("Subject is required");
        }

        if (replyTo != null && replyTo.length() == 0) {
            throw new IllegalArgumentException("ReplyTo cannot be the empty string");
        }

        kind = Kind.REGULAR;
        this.subject = subject;
        this.replyTo = replyTo;
        this.headers = headers;
        this.data = data;
        this.utf8mode = utf8mode;

        int replyToLen = replyTo == null ? 0 : replyTo.length();
        dataLen = data == null ? 0 : data.length;
        if (headers != null && !headers.isEmpty()) {
            hdrLen = headers.serializedLength();
        }
        else {
            hdrLen = 0;
        }
        totLen = hdrLen + dataLen;

        // initialize the builder with a reasonable length, preventing resize in 99.9% of the cases
        // 32 for misc + subject length doubled in case of utf8 mode + replyToLen + totLen (hdrLen + dataLen)
        ByteArrayBuilder bab = new ByteArrayBuilder(32 + (subject.length() * 2) + replyToLen + totLen);

        // protocol come first
        if (hdrLen > 0) {
            bab.append(HPUB_SP_BYTES);
        }
        else {
            bab.append(PUB_SP_BYTES);
        }

        // next comes the subject
        bab.append(subject, utf8mode ? UTF_8 : US_ASCII);
        bab.appendSpace();

        // reply to if it's there
        if (replyToLen > 0) {
            bab.append(replyTo);
            bab.appendSpace();
        }

        // header length if there are headers
        if (hdrLen > 0) {
            bab.append(Integer.toString(hdrLen));
            bab.appendSpace();
        }

        // payload length
        bab.append(Integer.toString(totLen));

        protocolBytes = bab.toByteArray();
    }

    // Create a protocol only message to publish
    NatsMessage(byte[] protocol) {
        this.kind = Kind.PROTOCOL;
        this.protocolBytes = protocol == null ? EMPTY_BODY : protocol;
    }

    // Create an incoming message for a subscriber
    // Doesn't check controlline size, since the server sent us the message
    NatsMessage(String sid, String subject, String replyTo, int protocolLength) {
        this.kind = Kind.INCOMING;
        this.sid = sid;
        this.subject = subject;
        this.replyTo = replyTo;
        this.protocolLineLength = protocolLength;
        // headers and data are set later and sizes are calculated during those setters
    }

    boolean isProtocol() {
        return kind == Kind.PROTOCOL;
    }

    // Will be null on an incoming message
    byte[] getProtocolBytes() {
        return this.protocolBytes;
    }

    int getControlLineLength() {
        return (this.protocolBytes != null) ? this.protocolBytes.length + 2 : -1;
    }

    long getSizeInBytes() {
        if (sizeInBytes == -1) {
            sizeInBytes = protocolLineLength;
            if (protocolBytes != null) {
                sizeInBytes += protocolBytes.length;
            }
            if (hdrLen > 0) {
                sizeInBytes += hdrLen + 2; // CRLF
            }
            if (data == null) {
                sizeInBytes += 2; // CRLF
            } else {
                sizeInBytes += dataLen + 4; // CRLF
            }
        }
        return sizeInBytes;
    }

    @Override
    public String getSID() {
        return this.sid;
    }

    void setReplyTo(String replyTo) {
        this.replyTo = replyTo;
    }

    // Only for incoming messages, with no protocol bytes
    void setHeaders(IncomingHeadersProcessor ihp) {
        headers = ihp.getHeaders();
        status = ihp.getStatus();
        hdrLen = ihp.getSerializedLength();
        totLen = hdrLen + dataLen;
    }

    // Only for incoming messages, with no protocol bytes
    void setData(byte[] data) {
        this.data = data;
        dataLen = data.length;
        totLen = hdrLen + dataLen;
    }

    void setSubscription(NatsSubscription sub) {
        this.subscription = sub;
    }

    NatsSubscription getNatsSubscription() {
        return this.subscription;
    }

    @Override
    public Connection getConnection() {
        return this.subscription == null ? null : this.subscription.connection;
    }

    @Override
    public String getSubject() {
        return this.subject;
    }

    @Override
    public String getReplyTo() {
        return this.replyTo;
    }

    byte[] getSerializedHeader() {
        return headers == null || headers.isEmpty() ? null : headers.getSerialized();
    }

    @Override
    public boolean hasHeaders() {
        return headers != null;
    }

    @Override
    public Headers getHeaders() {
        // create for adding post message creation.
        if (headers == null) {
            headers = new Headers();
        }
        return headers;
    }

    @Override
    public boolean hasStatus() {
        return status != null;
    }

    @Override
    public Status getStatus() {
        return status;
    }

    @Override
    public byte[] getData() {
        return this.data;
    }

    @Override
    public boolean isUtf8mode() {
        return utf8mode;
    }

    @Override
    public Subscription getSubscription() {
        return this.subscription;
    }

    private Connection getJetStreamValidatedConnection() {
        if (!this.isJetStream()) {
            throw new IllegalStateException("Message is not a jestream message");
        }

        if (getSubscription() == null) {
            throw new IllegalStateException("Messages is not bound to a subcription.");
        }

        Connection c = getConnection();
        if (c == null) {
            throw new IllegalStateException("Message is not bound to a connection");
        }
        return c;

    }

    private void ackReply(byte[] ackType) {
        try {
            ackReply(ackType, Duration.ZERO);
        } catch (InterruptedException e) {
            // we should never get here, but satisfy the linters.
            Thread.currentThread().interrupt();
        } catch (TimeoutException e) {
            // NOOP
        }
    }
    
    private boolean isPullMode() {
        if (!(this.subscription instanceof NatsJetStreamSubscription)) {
            return false;
        }
        return (((NatsJetStreamSubscription) this.subscription).pull > 0);
    }

    private void ackReply(byte[] ackType, Duration d) throws InterruptedException, TimeoutException {
        if (!this.isJetStream()) {
            throw new IllegalStateException("Message is not a jestream message");
        }
        if (d == null) {
            throw new IllegalArgumentException("Duration cannot be null.");
        }

        boolean isSync = (d != Duration.ZERO);
        Connection nc = getJetStreamValidatedConnection();

        if (isPullMode()) {
           if (Arrays.equals(ackType, AckAck)) {
              nc.publish(replyTo, subscription.getSubject(), AckNext);
           } else if (Arrays.equals(ackType, AckNak) || Arrays.equals(ackType, AckTerm)) {
                nc.publish(replyTo, subscription.getSubject(), AckNextOne);
           }
           if (isSync && nc.request(replyTo, null, d) == null) {
                throw new TimeoutException("Ack request next timed out.");
           }

        } else if (isSync && nc.request(replyTo, ackType, d) == null) {
            throw new TimeoutException("Ack response timed out.");
        } else {
            nc.publish(replyTo, ackType);
        }
    }

    @Override
    public void ack() {
        ackReply(AckAck);
    }

    @Override
    public void ackSync(Duration d) throws InterruptedException, TimeoutException {
        ackReply(AckAck, d);
    }

    @Override
    public void nak(){
        ackReply(AckNak);
    }

    @Override
    public void inProgress() {
        ackReply(AckProgress);
    }

    @Override
    public void term() {
        ackReply(AckTerm);
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
            if (!isJetStream()) {
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

    @Override
    public boolean isJetStream() {
        return replyTo != null && replyTo.startsWith("$JS");
    }
    
    @Override
    public String toString() {
        String hdrString = headers == null ? "" : new String(headers.getSerialized(), US_ASCII).replace("\r", "+").replace("\n", "+");
        return "NatsMessage:" +
                "\n  subject='" + subject + '\'' +
                "\n  replyTo='" + replyTo + '\'' +
                "\n  data=" + (data == null ? null : new String(data, UTF_8)) +
                "\n  utf8mode=" + utf8mode +
                "\n  headers=" + hdrString +
                "\n  sid='" + sid + '\'' +
                "\n  protocolLineLength=" + protocolLineLength +
                "\n  protocolBytes=" + (protocolBytes == null ? null : new String(protocolBytes, UTF_8)) +
                "\n  kind=" + kind +
                "\n  sizeInBytes=" + sizeInBytes +
                "\n  hdrLen=" + hdrLen +
                "\n  dataLen=" + dataLen +
                "\n  totLen=" + totLen +
                "\n  subscription=" + subscription +
                "\n  next=" + next;
    }

    /**
     * The builder is for building normal publish/request messages,
     * as an option for client use developers instead of the normal constructor
     */
    public static class Builder {
        String subject;
        String replyTo;
        Headers headers;
        byte[] data;
        boolean utf8mode;

        /**
         * Set the subject
         *
         * @param subject the subject
         * @return the builder
         */
        public Builder subject(final String subject) {
            this.subject = subject;
            return this;
        }

        /**
         * Set the reply to
         *
         * @param replyTo the reply to
         * @return the builder
         */
        public Builder replyTo(final String replyTo) {
            this.replyTo = replyTo;
            return this;
        }

        /**
         * Set the headers
         *
         * @param headers the headers
         * @return the builder
         */
        public Builder headers(final Headers headers) {
            this.headers = headers;
            return this;
        }

        /**
         * Set the data from a string
         *
         * @param data the data string
         * @param charset the charset, for example {@code StandardCharsets.UTF_8}
         * @return the builder
         */
        public Builder data(final String data, final Charset charset) {
            //
            this.data = data.getBytes(charset);
            return this;
        }

        /**
         * Set the data from a byte array. null data is left as is
         *
         * @param data the data
         * @return the builder
         */
        public Builder dataKeepNull(final byte[] data) {
            this.data = data;
            return this;
        }

        /**
         * Set the data from a byte array. null data changed to empty byte array
         *
         * @param data the data
         * @return the builder
         */
        public Builder dataOrEmpty(final byte[] data) {
            this.data = data == null ? EMPTY_BODY : data;
            return this;
        }

        /**
         * Set if the subject should be treated as utf
         *
         * @param utf8mode true if utf8 mode for subject
         * @return the builder
         */
        public Builder utf8mode(final boolean utf8mode) {
            this.utf8mode = utf8mode;
            return this;
        }

        /**
         * Build the {@code NatsMessage} object
         *
         * @return the {@code NatsMessage}
         */
        public NatsMessage build() {
            return new NatsMessage(subject, replyTo, headers, data, utf8mode);
        }
    }
}
