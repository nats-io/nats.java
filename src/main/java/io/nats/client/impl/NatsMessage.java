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
    private Integer protocolLineLength;

    // Kind.PROTOCOL : just this field
    private byte[] protocolBytes;

    // housekeeping
    private Kind kind;
    private int sizeInBytes = -1;
    private int hdrLen = 0;
    private int dataLen = 0;
    private int totLen = 0;

    private NatsSubscription subscription;

    NatsMessage next; // for linked list

    public NatsMessage(String subject, String replyTo, byte[] data, boolean utf8mode) {
        this(subject, replyTo, null, data, utf8mode);
    }

    public NatsMessage(Message message) {
        this(message.getSubject(), message.getReplyTo(),
                message.getHeaders(), message.getData(), message.isUtf8mode());
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
        this.data = data == null ? EMPTY_BODY : data;
        this.utf8mode = utf8mode;

        int replyToLen = replyTo == null ? 0 : replyTo.length();
        dataLen = this.data.length;
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
            sizeInBytes = protocolBytes == null ? 0 : protocolBytes.length + 2; // CRLF
            if (hdrLen > 0) {
                sizeInBytes += hdrLen + 2; // CRLF
            }
            if (dataLen > 0) {
                sizeInBytes += dataLen + 2; // CRLF
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
        return headers == null ? null : headers.getSerialized();
    }

    @Override
    public boolean hasHeaders() {
        return headers != null;
    }

    @Override
    public Headers getHeaders() {
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
        public Builder data(final String data, Charset charset) {
            //
            this.data = data.getBytes(charset);
            return this;
        }

        /**
         * Set the data from a byte array
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
