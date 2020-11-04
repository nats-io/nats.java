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

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Subscription;

class NatsMessage implements Message {
    private ByteBuffer sid;
    private ByteBuffer subject;
    private ByteBuffer replyTo;
    private ByteBuffer data;
    private ByteBuffer protocolBytes;
    private ByteBuffer messageBuffer = null;
    private NatsSubscription subscription;
    private Integer protocolLength = null;

    NatsMessage next; // for linked list

    // Create a message to publish
    NatsMessage(ByteBuffer subject, ByteBuffer replyTo, ByteBuffer data, boolean utf8mode) {
        // Calculate the length in bytes
        int size;
        int len = 6;
        if (data != null) {
            size = data.remaining();
            len += size + 2;
        } else {
            size = 0;
        }
        int dataSize = fastIntLength(size);
        len += dataSize;
        if (replyTo != null) {
            len += replyTo.remaining() + 1;
        }
        len += subject.remaining() + 1;
        this.messageBuffer = ByteBuffer.allocate(len);
        if (data != null)
            len -= (data.remaining() + 2);
        len -= 2;
        this.messageBuffer.limit(len);
        this.protocolBytes = this.messageBuffer.slice();
        this.messageBuffer.limit(subject.remaining() + 4);
        this.messageBuffer.position(4);
        this.subject = this.messageBuffer.slice();
        this.messageBuffer.clear();
        messageBuffer.put((byte)'P').put((byte)'U').put((byte)'B').put((byte)' ');
        messageBuffer.put(subject.asReadOnlyBuffer());
        messageBuffer.put((byte)' ');

        if (replyTo != null) {
            this.messageBuffer.mark();
            this.messageBuffer.limit(this.messageBuffer.position() + replyTo.remaining());
            this.replyTo = this.messageBuffer.slice();
            this.messageBuffer.reset();
            this.messageBuffer.limit(this.messageBuffer.capacity());
            messageBuffer.put(replyTo.asReadOnlyBuffer());
            messageBuffer.put((byte)' ');
        } else {
            this.replyTo = null;
        }

        if (size > 0) {
            int base = messageBuffer.position() + dataSize;
            for (int i = size; i > 0; i /= 10) {
                base--;
                messageBuffer.put(base, (byte)(i % 10 + (byte)'0'));
            }
            messageBuffer.position(messageBuffer.position() + dataSize);
        } else {
            messageBuffer.put((byte)'0');
        }
        messageBuffer.put((byte)'\r');
        messageBuffer.put((byte)'\n');

        if (data != null) {
            this.messageBuffer.mark();
            this.messageBuffer.limit(this.messageBuffer.position() + data.remaining());
            this.data = this.messageBuffer.slice();
            this.messageBuffer.reset();
            this.messageBuffer.limit(this.messageBuffer.capacity());
            messageBuffer.put(data.asReadOnlyBuffer());
            messageBuffer.put((byte)'\r');
            messageBuffer.put((byte)'\n');
        } else {
            this.data = null;
        }

        messageBuffer.clear();
    }

    NatsMessage(String subject, String replyTo, ByteBuffer data, boolean utf8mode) {
        this(NatsEncoder.encodeSubject(subject), (replyTo != null) ? NatsEncoder.encodeReplyTo(replyTo) : null, data, utf8mode);
    }

    NatsMessage(String subject, String replyTo, byte[] data, boolean utf8mode) {
        this(NatsEncoder.encodeSubject(subject), (replyTo != null) ? NatsEncoder.encodeReplyTo(replyTo) : null, ByteBuffer.wrap(data), utf8mode);
    }

    // Create a protocol only message to publish
    NatsMessage(CharBuffer protocol) {
        if (protocol.remaining() == 0) {
            this.protocolBytes = ByteBuffer.allocate(0);
        } else {
            protocol.mark();
            this.messageBuffer = ByteBuffer.allocate(fastUtf8Length(protocol) + 2);
            protocol.reset();
            NatsEncoder.encodeProtocol(protocol, this.messageBuffer, true);
            this.messageBuffer.flip();
            this.protocolBytes = this.messageBuffer.slice();
            this.messageBuffer.clear();
            this.messageBuffer.position(this.messageBuffer.limit() - 2);
            this.messageBuffer.put((byte)'\r');
            this.messageBuffer.put((byte)'\n');
            this.messageBuffer.flip();
        }
    }

    // Create a protocol only message to publish
    NatsMessage(ByteBuffer protocol) {
        this.messageBuffer = protocol;
        this.messageBuffer.limit(this.messageBuffer.limit() - 2);
        this.protocolBytes = this.messageBuffer.slice();
        this.messageBuffer.clear();
    }

    // Create an incoming message for a subscriber
    // Doesn't check controlline size, since the server sent us the message
    NatsMessage(ByteBuffer sid, ByteBuffer subject, ByteBuffer replyTo, int protocolLength) {
        this.sid = sid;
        this.subject = subject;
        if (replyTo != null) {
            this.replyTo = replyTo;
        }
        this.protocolLength = protocolLength;
        this.data = null; // will set data and size after we read it
    }

    NatsMessage(String sid, String subject, String replyTo, int protocolLength) {
        this(
                NatsEncoder.encodeSID(sid),
                NatsEncoder.encodeSubject(subject),
                (replyTo != null) ? NatsEncoder.encodeReplyTo(replyTo) : null,
                protocolLength
        );
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

    ByteBuffer getProtocolBuffer() {
        return this.protocolBytes;
    }

    // Will be null on an incoming message
    byte[] getProtocolBytes() {
        if (this.protocolBytes == null)
            return null;
        if (this.messageBuffer == null)
            return this.protocolBytes.array();
        byte[] bytes = new byte[this.protocolBytes.remaining()];
        this.protocolBytes.asReadOnlyBuffer().get(bytes);
        return bytes;
    }

    int getControlLineLength() {
        return (this.protocolBytes != null) ? this.protocolBytes.limit() + 2 : -1;
    }

    long getSizeInBytes() {
        if (this.messageBuffer != null)
            return this.messageBuffer.limit();
        long sizeInBytes = 0;
        if (this.protocolBytes != null) {
            sizeInBytes += this.protocolBytes.limit();
        }
        if (this.protocolLength != null){
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
        return NatsEncoder.decodeSID(this.sid.asReadOnlyBuffer());
    }

    public ByteBuffer getSIDBuffer() {
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
        return NatsEncoder.decodeSubject(this.subject.asReadOnlyBuffer());
    }

    public ByteBuffer getSubjectBuffer() {
        return this.subject;
    }

    public String getReplyTo() {
        if (this.replyTo == null)
            return null;

        return NatsEncoder.decodeReplyTo(this.replyTo.asReadOnlyBuffer());
    }

    public ByteBuffer getReplyToBuffer() {
        return this.replyTo;
    }

    public byte[] getData() {
        if (this.messageBuffer == null)
            return this.data.array();
        byte[] bytes = new byte[this.data.remaining()];
        this.data.asReadOnlyBuffer().get(bytes);
        return bytes;
    }

    public ByteBuffer getDataBuffer() {
        return this.data;
    }

    public ByteBuffer getMessageBuffer() {
        return this.messageBuffer;
    }

    public Subscription getSubscription() {
        return this.subscription;
    }
}
