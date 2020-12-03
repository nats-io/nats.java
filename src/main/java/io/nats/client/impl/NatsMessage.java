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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static io.nats.client.support.NatsConstants.*;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

public class NatsMessage implements Message {

    public enum Kind {REGULAR, PROTOCOL, INCOMING}

    // Kind.REGULAR : just these fields
    private String subject;
    private String replyTo;
    private ByteBuffer data;
    private boolean utf8mode;
    private Headers headers;
    private Status status;

    // Kind.INCOMING : subject, replyTo, data and these fields
    private String sid;
    private Integer protocolLineLength;

    // Kind.PROTOCOL : just this field
    private ByteBuffer protocolBytes;

    // housekeeping
    private final Kind kind;
    private int sizeInBytes = -1;
    private int hdrLen = 0;
    private int dataLen = 0;
    private int totLen = 0;

    private NatsSubscription subscription;

    NatsMessage next; // for linked list

    public NatsMessage(String subject, String replyTo, byte[] data, boolean utf8mode) {
        this(subject, replyTo, null, data, utf8mode);
    }

    public NatsMessage(String subject, String replyTo, ByteBuffer data, boolean utf8mode) {
        this(subject, replyTo, null, data, utf8mode);
    }

    public NatsMessage(Message message) {
        this(message.getSubject(), message.getReplyTo(),
                message.getHeaders(), message.getData(), message.isUtf8mode());
    }

    // Create a message to publish
    public NatsMessage(String subject, String replyTo, Headers headers, ByteBuffer data, boolean utf8mode) {
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
        Charset charset;

        // Calculate the length in bytes
        int len = 0;
        if (headers != null && !headers.isEmpty()) {
            hdrLen = headers.serializedLength();
            len += OP_HPUB_SP_LEN;
            len += fastIntLength(hdrLen);
            len += 1;
        }
        else {
            len += OP_PUB_SP_LEN;
        }
        int size = (data != null) ? data.limit() : 0;
        size += hdrLen;
        int sizeLen = fastIntLength(size);
        len += sizeLen;

        if (replyTo != null) {
            if (utf8mode) {
                len += fastUtf8Length(replyTo) + 1;
            } else {
                len += replyTo.length() + 1;
            }
        }
        if (utf8mode) {
            len += fastUtf8Length(subject) + 1;
            charset = UTF_8;
        } else {
            len += subject.length() + 1;
            charset = US_ASCII;
        }
        this.protocolBytes = ByteBuffer.allocate(len);

        // protocol come first
        if (hdrLen > 0) {
            protocolBytes.put(HPUB_SP_BYTES.asReadOnlyBuffer());
        } else {
            protocolBytes.put(PUB_SP_BYTES.asReadOnlyBuffer());
        }

        protocolBytes.put(subject.getBytes(charset));
        protocolBytes.put((byte)' ');

        if (replyTo != null) {
            protocolBytes.put(replyTo.getBytes(charset));
            protocolBytes.put((byte)' ');
        }


        // header length if there are headers
        if (hdrLen > 0) {
            protocolBytes.put(US_ASCII.encode(Integer.toString(hdrLen)));
            protocolBytes.put((byte)' ');
        }

        if (size > 0) {
            int base = protocolBytes.position() + sizeLen;
            for (int i = size; i > 0; i /= 10) {
                base--;
                protocolBytes.put(base, (byte)(i % 10 + (byte)'0'));
            }
            protocolBytes.position(protocolBytes.position() + sizeLen);
        } else {
            protocolBytes.put((byte)'0');
        }
        protocolBytes.flip();
    }

    NatsMessage(String subject, String replyTo, Headers headers, byte[] data, boolean utf8mode) {
        this(subject, replyTo, headers, (data != null) ? ByteBuffer.wrap(data) : null, utf8mode);
    }

    // Create a protocol only message to publish
    NatsMessage(ByteBuffer protocol) {
        this.kind = Kind.PROTOCOL;
        this.protocolBytes = protocol == null ? EMPTY_BODY_BUFFER : protocol;
    }

    // Create a protocol only message to publish
    NatsMessage(byte[] protocol) {
        this((protocol != null) ? ByteBuffer.wrap(protocol) : null);
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

    Kind getKind() {
        return kind;
    }

    boolean isProtocol() {
        return kind == Kind.PROTOCOL;
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
        if (this.protocolLineLength != null){
            sizeInBytes += this.protocolLineLength;
        }
        if (data != null) {
            sizeInBytes += data.limit() + 4;// for 2x \r\n
        } else {
            sizeInBytes += 2;
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
        this.data = ByteBuffer.wrap(data);
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
        if (this.data == null)
            return new byte[0];
        return this.data.array();
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
                "\n  data=" + (data == null ? null : UTF_8.decode(data.asReadOnlyBuffer()).toString()) +
                "\n  utf8mode=" + utf8mode +
                "\n  headers=" + hdrString +
                "\n  sid='" + sid + '\'' +
                "\n  protocolLineLength=" + protocolLineLength +
                "\n  protocolBytes=" + (protocolBytes == null ? null : UTF_8.decode(protocolBytes.asReadOnlyBuffer()).toString()) +
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
