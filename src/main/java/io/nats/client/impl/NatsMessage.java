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

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Subscription;
import io.nats.client.support.ByteArrayBuilder;
import io.nats.client.support.IncomingHeadersProcessor;
import io.nats.client.support.JsPrefixManager;
import io.nats.client.support.Status;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

import static io.nats.client.support.NatsConstants.*;
import static io.nats.client.support.Validator.validateReplyTo;
import static io.nats.client.support.Validator.validateSubject;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

public class NatsMessage implements Message {

    protected static final String NOT_A_JET_STREAM_MESSAGE = "Message is not a JetStream message";

    protected String subject;
    protected String replyTo;
    protected byte[] data;
    protected boolean utf8mode;
    protected Headers headers;

    // incoming specific : subject, replyTo, data and these fields
    protected String sid;
    protected int protocolLineLength;

    // protocol specific : just this field
    protected byte[] protocolBytes;

    // housekeeping
    protected int sizeInBytes = -1;
    protected int hdrLen = 0;
    protected int dataLen = 0;
    protected int totLen = 0;

    protected boolean dirty = false;

    protected NatsSubscription subscription;

    NatsMessage next; // for linked list

    // ----------------------------------------------------------------------------------------------------
    // Constructors - Prefer to use Builder
    // ----------------------------------------------------------------------------------------------------
    private NatsMessage() {
        this.data = EMPTY_BODY;
    }

    private NatsMessage(byte[] data) {
        this.data = data == null ? EMPTY_BODY : data;
    }

    @Deprecated // Plans are to remove allowing utf8mode
    public NatsMessage(String subject, String replyTo, byte[] data, boolean utf8mode) {
        this(subject, replyTo, null, data, utf8mode);
    }

    public NatsMessage(String subject, String replyTo, byte[] data) {
        this(subject, replyTo, null, data, false);
    }

    public NatsMessage(Message message) {
        this(message.getSubject(),
                message.getReplyTo(),
                message.getHeaders(),
                message.getData(),
                message.isUtf8mode());
    }


    @Deprecated // Plans are to remove allowing utf8mode
    public NatsMessage(String subject, String replyTo, Headers headers, byte[] data, boolean utf8mode) {
        this(subject, replyTo, headers, data);
        this.utf8mode = utf8mode;
    }

    public NatsMessage(String subject, String replyTo, Headers headers, byte[] data) {
        this(data);
        this.subject = validateSubject(subject, true);
        this.replyTo = validateReplyTo(replyTo, false);
        this.headers = headers;
        this.utf8mode = false;

        dirty = true;
    }

    // ----------------------------------------------------------------------------------------------------
    // Only for implementors. The user facing message is the only current one that calculates.
    // ----------------------------------------------------------------------------------------------------
    protected boolean calculateIfDirty() {
        if (dirty || (hasHeaders() && headers.isDirty())) {
            int replyToLen = replyTo == null ? 0 : replyTo.length();
            dataLen = data.length;

            if (headers != null && !headers.isEmpty()) {
                hdrLen = headers.serializedLength();
            } else {
                hdrLen = 0;
            }
            totLen = hdrLen + dataLen;

            // initialize the builder with a reasonable length, preventing resize in 99.9% of the cases
            // 32 for misc + subject length doubled in case of utf8 mode + replyToLen + totLen (hdrLen + dataLen)
            ByteArrayBuilder bab = new ByteArrayBuilder(32 + (subject.length() * 2) + replyToLen + totLen);

            // protocol come first
            if (hdrLen > 0) {
                bab.append(HPUB_SP_BYTES);
            } else {
                bab.append(PUB_SP_BYTES);
            }

            // next comes the subject
            bab.append(subject, utf8mode ? UTF_8 : US_ASCII).append(SP);

            // reply to if it's there
            if (replyToLen > 0) {
                bab.append(replyTo).append(SP);
            }

            // header length if there are headers
            if (hdrLen > 0) {
                bab.append(Integer.toString(hdrLen)).append(SP);
            }

            // payload length
            bab.append(Integer.toString(totLen));

            protocolBytes = bab.toByteArray();
            dirty = false;
            return true;
        }
        return false;
    }

    // ----------------------------------------------------------------------------------------------------
    // Client and Message Internal Methods
    // ----------------------------------------------------------------------------------------------------
    long getSizeInBytes() {
        if (calculateIfDirty() || sizeInBytes == -1) {
            sizeInBytes = protocolLineLength;
            if (protocolBytes != null) {
                sizeInBytes += protocolBytes.length;
            }
            if (hdrLen > 0) {
                sizeInBytes += hdrLen + 2; // CRLF
            }
            if (data.length == 0) {
                sizeInBytes += 2; // CRLF
            } else {
                sizeInBytes += dataLen + 4; // CRLF
            }
        }
        return sizeInBytes;
    }

    boolean isProtocol() {
        return false; // overridden in NatsMessage.ProtocolMessage
    }

    byte[] getProtocolBytes() {
        calculateIfDirty();
        return protocolBytes;
    }

    int getControlLineLength() {
        calculateIfDirty();
        return (protocolBytes != null) ? protocolBytes.length + 2 : -1;
    }

    Headers getOrCreateHeaders() {
        if (headers == null) {
            headers = new Headers();
        }
        return headers;
    }

    void setSubscription(NatsSubscription sub) {
        subscription = sub;
    }

    NatsSubscription getNatsSubscription() {
        return subscription;
    }

    // ----------------------------------------------------------------------------------------------------
    // Public Interface Methods
    // ----------------------------------------------------------------------------------------------------
    @Override
    public String getSID() {
        return sid;
    }

    @Override
    public Connection getConnection() {
        return subscription == null ? null : subscription.connection;
    }

    @Override
    public String getSubject() {
        return subject;
    }

    @Override
    public String getReplyTo() {
        return replyTo;
    }

    byte[] getSerializedHeader() {
        return hasHeaders() ? headers.getSerialized() : null;
    }

    @Override
    public boolean hasHeaders() {
        return headers != null && !headers.isEmpty();
    }

    @Override
    public Headers getHeaders() {
        return headers;
    }

    @Override
    public boolean isStatusMessage() {
        return false;
    }

    @Override
    public Status getStatus() {
        return null;
    }

    @Override
    public byte[] getData() {
        return data;
    }

    @Override
    public boolean isUtf8mode() {
        return utf8mode;
    }

    @Override
    public Subscription getSubscription() {
        return subscription;
    }

    @Override
    public void ack() {
        throw new IllegalStateException(NOT_A_JET_STREAM_MESSAGE);
    }

    @Override
    public void ackSync(Duration d) throws InterruptedException, TimeoutException {
        throw new IllegalStateException(NOT_A_JET_STREAM_MESSAGE);
    }

    @Override
    public void nak() {
        throw new IllegalStateException(NOT_A_JET_STREAM_MESSAGE);
    }

    @Override
    public void inProgress() {
        throw new IllegalStateException(NOT_A_JET_STREAM_MESSAGE);
    }

    @Override
    public void term() {
        throw new IllegalStateException(NOT_A_JET_STREAM_MESSAGE);
    }

    @Override
    public NatsJetStreamMetaData metaData() {
        throw new IllegalStateException(NOT_A_JET_STREAM_MESSAGE);
    }

    @Override
    public boolean isJetStream() {
        return false;  // overridden in NatsJetStreamMessage
    }

    public ByteArrayBuilder appendSerialized(ByteArrayBuilder bab) {
        bab.append(getProtocolBytes()).append(CRLF_BYTES);

        if (!isProtocol()) {
            if (hasHeaders()) {
                getHeaders().appendSerialized(bab);
            }

            if (getData().length > 0) {
                bab.append(getData());
            }

            bab.append(CRLF_BYTES);
        }

        return bab;
    }

    @Override
    public String toString() {
        if (subject == null) {
            return "NatsMessage | " + new String(protocolBytes);
        }
        return "NatsMessage |" + subject + "|" + replyToString() + "|" + dataToString() + "|";
    }

    String toDetailString() {
        calculateIfDirty();
        return "NatsMessage:" +
                "\n  subject='" + subject + '\'' +
                "\n  replyTo='" + replyToString() + '\'' +
                "\n  data=" + dataToString() +
                "\n  utf8mode=" + utf8mode +
                "\n  headers=" + headersToString() +
                "\n  sid='" + sid + '\'' +
                "\n  protocolLineLength=" + protocolLineLength +
                "\n  protocolBytes=" + protocolBytesToString() +
                "\n  sizeInBytes=" + sizeInBytes +
                "\n  hdrLen=" + hdrLen +
                "\n  dataLen=" + dataLen +
                "\n  totLen=" + totLen +
                "\n  subscription=" + subscription +
                "\n  next=" + nextToString();

    }

    private String headersToString() {
        return hasHeaders() ? new String(headers.getSerialized(), US_ASCII).replace("\r", "+").replace("\n", "+") : "";
    }

    private String dataToString() {
        return data.length == 0 ? "<no data>" : new String(data, UTF_8);
    }

    private String replyToString() {
        return replyTo == null ? "<no reply>" : replyTo;
    }

    private String protocolBytesToString() {
        return protocolBytes == null ? null : new String(protocolBytes, UTF_8);
    }

    private String nextToString() {
        return next == null ? "No" : "Yes";
    }

    // ----------------------------------------------------------------------------------------------------
    // Standard Builder
    // ----------------------------------------------------------------------------------------------------
    public static Builder builder() {
        return new Builder();
    }

    /**
     * The builder is for building normal publish/request messages,
     * as an option for client use developers instead of the normal constructor
     */
    public static class Builder {
        private String subject;
        private String replyTo;
        private Headers headers;
        private byte[] data;
        private boolean utf8mode;

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
         * Set the data from a string converting using the
         * charset StandardCharsets.UTF_8
         *
         * @param data    the data string
         * @return the builder
         */
        public Builder data(final String data) {
            if (data != null) {
                this.data = data.getBytes(StandardCharsets.UTF_8);
            }
            return this;
        }

        /**
         * Set the data from a string
         *
         * @param data    the data string
         * @param charset the charset, for example {@code StandardCharsets.UTF_8}
         * @return the builder
         */
        public Builder data(final String data, final Charset charset) {
            this.data = data.getBytes(charset);
            return this;
        }

        /**
         * Set the data from a byte array. null data changed to empty byte array
         *
         * @param data the data
         * @return the builder
         */
        public Builder data(final byte[] data) {
            this.data = data;
            return this;
        }

        /**
         * Set if the subject should be treated as utf
         *
         * @deprecated Plans are to remove allowing utf8mode
         * @param utf8mode true if utf8 mode for subject
         * @return the builder
         */
        @Deprecated // Plans are to remove allowing utf8mode
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

    // ----------------------------------------------------------------------------------------------------
    // Incoming Message Factory - internal use only
    // ----------------------------------------------------------------------------------------------------
    static class InternalMessageFactory {
        private final String sid;
        private final String subject;
        private final String replyTo;
        private final int protocolLineLength;
        private final boolean utf8mode;

        private byte[] data;
        private Headers headers;
        private Status status;
        private int hdrLen = 0;
        private int dataLen = 0;
        private int totLen = 0;

        // Create an incoming message for a subscriber
        // Doesn't check control line size, since the server sent us the message
        InternalMessageFactory(String sid, String subject, String replyTo, int protocolLength, boolean utf8mode) {
            this.sid = sid;
            this.subject = subject;
            this.replyTo = replyTo;
            this.protocolLineLength = protocolLength;
            this.utf8mode = utf8mode;
            // headers and data are set later and sizes are calculated during those setters
        }

        void setHeaders(IncomingHeadersProcessor ihp) {
            headers = ihp.getHeaders();
            status = ihp.getStatus();
            hdrLen = ihp.getSerializedLength();
            totLen = hdrLen + dataLen;
        }

        void setData(byte[] data) {
            this.data = data;
            dataLen = data == null ? 0 : data.length;
            totLen = hdrLen + dataLen;
        }

        NatsMessage getMessage() {
            NatsMessage message = null;
            if (status != null) {
                message = new StatusMessage(status);
            }
            else if (JsPrefixManager.hasPrefix(replyTo)) {
                message = new NatsJetStreamMessage();
            }
            if (message == null) {
                message = new InternalMessage();
            }
            message.sid = this.sid;
            message.subject = this.subject;
            message.replyTo = this.replyTo;
            message.protocolLineLength = this.protocolLineLength;
            message.headers = this.headers;
            message.data = this.data == null ? EMPTY_BODY : this.data;
            message.utf8mode = this.utf8mode;
            message.hdrLen = this.hdrLen;
            message.dataLen = this.dataLen;
            message.totLen = this.totLen;

            return message;
        }
    }

    static class InternalMessage extends NatsMessage {
        @Override
        protected boolean calculateIfDirty() {
            return false;
        }
    }

    static class ProtocolMessage extends InternalMessage {
        ProtocolMessage(byte[] protocol) {
            this.protocolBytes = protocol == null ? EMPTY_BODY : protocol;
        }

        ProtocolMessage(ByteArrayBuilder babProtocol) {
            this(babProtocol.toByteArray());
        }

        ProtocolMessage(String asciiProtocol) {
            this(asciiProtocol.getBytes(StandardCharsets.US_ASCII));
        }

        @Override
        boolean isProtocol() {
            return true;
        }
    }

    static class StatusMessage extends InternalMessage {
        private final Status status;

        public StatusMessage(Status status) {
            this.status = status;
        }

        @Override
        public boolean isStatusMessage() {
            return true;
        }

        @Override
        public Status getStatus() {
            return status;
        }

        @Override
        public String toString() {
            return "StatusMessage{" +
                    "code=" + status.getCode() +
                    ", message='" + status.getMessage() + '\'' +
                    '}';
        }
    }
}
