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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

class NatsMessage implements Message {
    public enum Kind {REGULAR, PROTOCOL, INCOMING}

    private final Kind kind;

    // Kind.REGULAR : just these fields
    private String subject;
    private String replyTo;
    private ByteBuffer data;
    private boolean utf8mode;

    // Kind.INCOMING : subject, replyTo, data and these fields
    private String sid;
    private Integer protocolLineLength;

    // Kind.PROTOCOL : just this field
    private ByteBuffer protocolBytes;

    private NatsSubscription subscription;

    NatsMessage next; // for linked list

    private NatsMessage(Builder builder) {
        kind = builder.kind;
        if (kind == Kind.REGULAR) {
            regular(builder);
        }
        else if (kind == Kind.INCOMING){
            incoming(builder);
        }
        else {
            protocol(builder);
        }
    }

    private void regular(Builder builder) {
        subject = builder.subject;
        replyTo = builder.replyTo;
        data = builder.data;
        Charset charset;

        // Calculate the length in bytes
        int size = (builder.data != null) ? builder.data.limit() : 0;
        int len = fastIntLength(size) + 4;
        if (builder.replyTo != null) {
            if (builder.utf8mode) {
                len += fastUtf8Length(builder.replyTo) + 1;
            } else {
                len += builder.replyTo.length() + 1;
            }
        }
        if (builder.utf8mode) {
            len += fastUtf8Length(builder.subject) + 1;
            charset = StandardCharsets.UTF_8;
        } else {
            len += builder.subject.length() + 1;
            charset = StandardCharsets.US_ASCII;
        }
        protocolBytes = ByteBuffer.allocate(len);
        protocolBytes.put((byte)'P').put((byte)'U').put((byte)'B').put((byte)' ');
        protocolBytes.put(builder.subject.getBytes(charset));
        protocolBytes.put((byte)' ');

        if (builder.replyTo != null) {
            protocolBytes.put(builder.replyTo.getBytes(charset));
            protocolBytes.put((byte)' ');
        }

        if (size > 0) {
            int base = protocolBytes.limit();
            for (int i = size; i > 0; i /= 10) {
                base--;
                protocolBytes.put(base, (byte)(i % 10 + (byte)'0'));
            }
        } else {
            protocolBytes.put((byte)'0');
        }
        protocolBytes.clear();
    }

    private void incoming(Builder builder) {
        // Create an incoming message for a subscriber
        // Doesn't check control line size, since the server sent us the message
        sid = builder.sid;
        subject = builder.subject;
        replyTo = builder.replyTo;
        protocolLineLength = builder.protocolLineLength;
        data = builder.data;
    }

    private void protocol(Builder builder) {
        if (builder.protocolBuffer.remaining() == 0) {
            protocolBytes = ByteBuffer.allocate(0);
        } else {
            builder.protocolBuffer.mark();
            protocolBytes = ByteBuffer.allocate(fastUtf8Length(builder.protocolBuffer));
            builder.protocolBuffer.reset();
            StandardCharsets.UTF_8.newEncoder().encode(builder.protocolBuffer, protocolBytes, true);
            protocolBytes.clear();
        }
    }

    // Create a protocol only message to publish
    public static class Builder {
        private Kind kind; // only incoming and protocol will set this. If null at build(), it means REGULAR

        // Kind.REGULAR : just these fields
        private String subject;
        private String replyTo;
        private ByteBuffer data = ByteBuffer.wrap(new byte[0]);
        private boolean utf8mode;

        // Kind.INCOMING : subject, replyTo, data and these fields
        private String sid;
        private Integer protocolLineLength;

        // Kind.PROTOCOL : just this field
        private CharBuffer protocolBuffer;

        public Builder subject(final String subject) {
            initKindIfNotInit("subject");
            this.subject = subject;
            return this;
        }

        public Builder replyTo(final String replyTo) {
            initKindIfNotInit("replyTo");
            this.replyTo = replyTo;
            return this;
        }

        public Builder data(final ByteBuffer data) {
            initKindIfNotInit("data");
            this.data = data;
            return this;
        }

        public Builder data(final byte[] data) {
            return data(ByteBuffer.wrap(data));
        }

        public Builder utf8mode(final boolean utf8mode) {
            initKindIfNotInit("utf8mode");
            this.utf8mode = utf8mode;
            return this;
        }

        public Builder sid(final String sid) {
            updateKindToIncoming("sid");
            this.sid = sid;
            return this;
        }

        public Builder protocolLineLength(final int protocolLineLength) {
            updateKindToIncoming("protocolLineLength");
            this.protocolLineLength = protocolLineLength;
            return this;
        }

        private void initKindIfNotInit(String field) {
            cannotAlreadyBeProtocol(field);
            if (kind == null) {
                kind = Kind.REGULAR;
            }
        }

        private void updateKindToIncoming(String field) {
            cannotAlreadyBeProtocol(field);
            kind = Kind.INCOMING;
        }

        private void cannotAlreadyBeProtocol(String field) {
            if (kind == Kind.PROTOCOL) {
                throw new IllegalStateException("Builder cannot accept '" + field + "' once started as another kind.");
            }
        }

        public Builder protocol(final CharBuffer buffer) {
            if (kind == null || kind == Kind.PROTOCOL) {
                kind = Kind.PROTOCOL;
                this.protocolBuffer = buffer;
                return this;
            }

            throw new IllegalStateException("Builder cannot accept 'protocol' once started as another kind.");
        }

        public Builder protocol(final String protocol) {
            return protocol(CharBuffer.wrap(protocol));
        }

        public NatsMessage build() {
            if (kind == null) {
                throw new IllegalStateException("Builder has not been set with any values.");
            }
            return new NatsMessage(this);
        }
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

    public Kind getKind() {
        return kind;
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
}
