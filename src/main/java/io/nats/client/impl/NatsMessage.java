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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

public class NatsMessage implements Message {
    private String sid;
    private String subject;
    private String replyTo;
    private Headers headers;
    private boolean utf8mode;
    private byte[] data;
    private byte[] protocolBytes;
    private NatsSubscription subscription;
    private long sizeInBytes;

    NatsMessage next; // for linked list

    static final byte[] digits = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

    private static final String PUB_SPACE = NatsConnection.OP_PUB + " ";
    private static final String HPUB_SPACE = NatsConnection.OP_HPUB + " ";
    private static final String SPACE = " ";
    private static final String CRLF = "\r\n";
    private static final String COLON_SPACE = ": ";

    private NatsMessage() {}

    private abstract static class Builder<T> {
        protected String subject;
        protected String replyTo;
        protected Headers headers;
        protected boolean utf8mode;
        protected byte[] data = Message.EMPTY_BODY;

        public T subject(String subject)    { this.subject = subject; return (T)this; }
        public T replyTo(String replyTo)    { this.replyTo = replyTo; return (T)this; }
        public T utf8mode(boolean utf8mode) { this.utf8mode = utf8mode; return (T)this; }
        public T headers(Headers headers)   { this.headers = headers; return (T)this; }

        private Headers getHeaders() {
            if (headers == null) {
                headers = new Headers();
            }
            return headers;
        }

        public T addHeader(String key, String value) { getHeaders().add(key, value); return (T)this; }
        public T addHeader(String key, Collection<String> values) { getHeaders().add(key, values); return (T)this; }

        public T data(byte[] data) {
            this.data = data == null ? Message.EMPTY_BODY : data;
            return (T)this;
        }

        public T data(String data, Charset charset) {
            this.data = data == null ? Message.EMPTY_BODY : data.getBytes(charset);
            return (T)this;
        }

        public NatsMessage build() {
            NatsMessage msg = new NatsMessage();
            msg.subject = subject;
            msg.replyTo = replyTo;
            msg.data = data;
            msg.utf8mode = utf8mode;
            msg.headers = headers;
            return msg;
        }
    }

    public static class PublishBuilder extends Builder<PublishBuilder> {
        private Long maxPayload;

        public PublishBuilder maxPayload(Long maxPayload) { this.maxPayload = maxPayload; return this; }

        public NatsMessage build() {
            NatsMessage msg = super.build();

            if (subject == null || subject.length() == 0) {
                throw new IllegalArgumentException("Subject is required in publish");
            }

            if (replyTo != null && replyTo.length() == 0) {
                throw new IllegalArgumentException("ReplyTo cannot be the empty string");
            }

            if (data.length > 0) {
                if (maxPayload == null) {
                    throw new IllegalArgumentException("Max Payload must be set before build is called when there is data.");
                }

                if (data.length > maxPayload && maxPayload > 0) {
                    throw new IllegalArgumentException(
                            "Message payload size exceed server configuration " + data.length + " vs " + maxPayload);
                }
            }

            boolean hpub = headers != null && !headers.isEmpty();

            if (utf8mode || hpub) {
                int subjectSize = subject.length() * 2;
                int replySize = (replyTo != null) ? replyTo.length() * 2 : 0;
                StringBuilder protocolStringBuilder = new StringBuilder(4 + subjectSize + 1 + replySize + 1);
                if (hpub) {
                    protocolStringBuilder.append(HPUB_SPACE);
                } else {
                    protocolStringBuilder.append(PUB_SPACE);
                }
                protocolStringBuilder.append(subject);
                protocolStringBuilder.append(SPACE);
                if (replyTo != null) {
                    protocolStringBuilder.append(replyTo);
                    protocolStringBuilder.append(SPACE);
                }
                if (hpub) {
                    int headerLength = calculateHeaderLength(headers);
                    protocolStringBuilder.append(headerLength);
                    protocolStringBuilder.append(SPACE);
                    protocolStringBuilder.append(data.length + headerLength);
                    protocolStringBuilder.append(CRLF);
                    outputHeaders(headers, protocolStringBuilder);
                    msg.headers = headers;
                } else {
                    protocolStringBuilder.append(data.length);
                }
                msg.protocolBytes = protocolStringBuilder.toString().getBytes(StandardCharsets.UTF_8);
            } else {

                // Convert the length to bytes
                byte[] lengthBytes = new byte[12];
                int idx = lengthBytes.length;
                int size = (data != null) ? data.length : 0;

                if (size > 0) {
                    for (int i = size; i > 0; i /= 10) {
                        idx--;
                        lengthBytes[idx] = digits[i % 10];
                    }
                } else {
                    idx--;
                    lengthBytes[idx] = digits[0];
                }

                // Build the array
                int len = 4 + subject.length() + 1 + (lengthBytes.length - idx);

                if (replyTo != null) {
                    len += replyTo.length() + 1;
                }

                msg.protocolBytes = new byte[len];

                // Copy everything
                int pos = 0;
                msg.protocolBytes[0] = 'P';
                msg.protocolBytes[1] = 'U';
                msg.protocolBytes[2] = 'B';
                msg.protocolBytes[3] = ' ';
                pos = 4;
                pos = copy(msg.protocolBytes, pos, subject);
                msg.protocolBytes[pos] = ' ';
                pos++;

                if (replyTo != null) {
                    pos = copy(msg.protocolBytes, pos, replyTo);
                    msg.protocolBytes[pos] = ' ';
                    pos++;
                }

                System.arraycopy(lengthBytes, idx, msg.protocolBytes, pos, lengthBytes.length - idx);
            }

            msg.sizeInBytes = msg.protocolBytes.length + data.length + 4;// for 2x \r\n

            return msg;
        }

        private void outputHeaders(Headers headers, StringBuilder protocolStringBuilder) {
            if (headers != null) {
                headers.forEach(header -> {
                    header.getValues().forEach(value -> {
                        protocolStringBuilder.append(header.getKey());
                        protocolStringBuilder.append(COLON_SPACE);
                        protocolStringBuilder.append(value);
                        protocolStringBuilder.append(CRLF);
                    });
                });
            }
            protocolStringBuilder.append(CRLF);
        }

        private int calculateHeaderLength(Headers headers) {
            AtomicInteger headerLength = new AtomicInteger(2); //closing \r\n
            if (headers != null) {
                headers.forEach(header -> {
                    // each line will have the key, colon, space, value, \r\n.
                    int eachLen = header.getKey().length() + 4; // precalculate all but each value
                    header.getValues().forEach(v -> headerLength.addAndGet(eachLen + v.length()) );
                });
            }
            return headerLength.get();
        }
    }

    // Create a protocol only message to publish
    NatsMessage(CharBuffer protocol) {
        ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(protocol);
        this.protocolBytes = Arrays.copyOfRange(byteBuffer.array(), byteBuffer.position(), byteBuffer.limit());
        Arrays.fill(byteBuffer.array(), (byte) 0); // clear sensitive data
        this.sizeInBytes = this.protocolBytes.length + 2;// for \r\n
    }

    public static class IncomingBuilder extends Builder<IncomingBuilder> {
        private String sid;
        private long protocolLineLength;

        public IncomingBuilder sid(String sid) { this.sid = sid; return this; }
        public IncomingBuilder protocolLength(int len) { this.protocolLineLength = len; return this; }

        @Override
        public NatsMessage build() {
            NatsMessage msg = super.build();
            msg.sid = sid;
            msg.sizeInBytes = protocolLineLength + 2 + data.length + 2; // for \r\n
            return msg;
        }
    }

    boolean isProtocol() {
        return this.subject == null;
    }

    public int getControlLineLength() {
        return (this.protocolBytes != null) ? this.protocolBytes.length + 2 : -1;
    }

    void setSubscription(NatsSubscription sub) {
        this.subscription = sub;
    }

    NatsSubscription getNatsSubscription() {
        return subscription;
    }

    @Override
    public Connection getConnection() {
        return subscription == null ? null : subscription.connection;
    }

    @Override
    public String getSubject() {
        return this.subject;
    }

    @Override
    public String getReplyTo() {
        return this.replyTo;
    }

    @Override
    public Headers getHeaders() {
        return headers;
    }

    @Override
    public boolean isUtf8mode() {
        return utf8mode;
    }

    @Override
    public byte[] getData() {
        return this.data;
    }

    @Override
    public String getSID() {
        return this.sid;
    }

    @Override
    public NatsSubscription getSubscription() {
        return this.subscription;
    }

    @Override
    public byte[] getProtocolBytes() {
        return this.protocolBytes;
    }

    @Override
    public long getSizeInBytes() {
        return sizeInBytes;
    }

    private static int copy(byte[] dest, int pos, String toCopy) {
        for (int i = 0, max = toCopy.length(); i < max; i++) {
            dest[pos] = (byte) toCopy.charAt(i);
            pos++;
        }

        return pos;
    }
}
