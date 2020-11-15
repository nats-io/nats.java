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

import java.nio.CharBuffer;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

class NatsMessage implements Message {
    private static byte[] PUB_BYTES = "PUB ".getBytes(US_ASCII);
    private static byte[] HPUB_BYTES = "HPUB ".getBytes(US_ASCII);
    private static byte[] SPACE = " ".getBytes(US_ASCII);
    private static int PUB_BYTES_LEN = PUB_BYTES.length;
    private static int HPUB_BYTES_LEN = HPUB_BYTES.length;
    private static int SPACE_LEN = SPACE.length;

    public enum Kind {REGULAR, PROTOCOL, INCOMING}

    // Kind.REGULAR : just these fields
    private String subject;
    private String replyTo;
    private byte[] data;
    private boolean utf8mode;
    private Headers headers;

    // Kind.INCOMING : subject, replyTo, data and these fields
    private String sid;
    private Integer protocolLineLength;

    // Kind.PROTOCOL : just this field
    private byte[] protocolBytes;

    // housekeeping
    private Kind kind;
    private boolean hpub = false;
    private int sizeInBytes = -1;
    private int hdrLen = 0;
    private int dataLen = 0;
    private int totLen = 0;

    private NatsSubscription subscription;

    NatsMessage next; // for linked list

    NatsMessage(String subject, String replyTo, byte[] data, boolean utf8mode) {
        this(subject, replyTo, null, data, utf8mode);
    }

    // Create a message to publish
    NatsMessage(String subject, String replyTo, Headers headers, byte[] data, boolean utf8mode) {
        kind = Kind.REGULAR;
        this.subject = subject;
        this.replyTo = replyTo;
        this.headers = headers;
        this.data = data;
        dataLen = data == null ? 0 : data.length;
        this.utf8mode = utf8mode;
        hpub = headers != null;

        ByteArrayBuilder bab = new ByteArrayBuilder();
        if (hpub && headers != null) {
            hdrLen = headers.serializedLength();
            bab.append(HPUB_BYTES, HPUB_BYTES_LEN);
        }
        else {
            hdrLen = 0;
            bab.append(PUB_BYTES, PUB_BYTES_LEN);
        }
        totLen = hdrLen + dataLen;

        // next comes the subject
        bab.append(subject, utf8mode ? UTF_8 : US_ASCII);
        bab.append(SPACE, SPACE_LEN);

        // reply to if it's there
        if (replyTo != null && replyTo.length() > 0) {
            bab.append(replyTo);
            bab.append(SPACE, SPACE_LEN);
        }

        // header length if there are headers
        if (hpub) {
            bab.append(Integer.toString(hdrLen));
            bab.append(SPACE, SPACE_LEN);
        }

        // payload length
        bab.append(Integer.toString(totLen));

        protocolBytes = bab.toByteArray();
    }

    // Create a protocol only message to publish
    NatsMessage(byte[] protocol) {
        this.kind = Kind.PROTOCOL;
        this.protocolBytes = protocol == null ? new byte[0] : protocol;
    }

    NatsMessage(String protocol) {
        this(protocol.getBytes(US_ASCII));
    }

    NatsMessage(CharBuffer protocol) {
        this(protocol.toString().getBytes(US_ASCII));
    }

    // Create an incoming message for a subscriber
    // Doesn't check controlline size, since the server sent us the message
    NatsMessage(String sid, String subject, String replyTo, int protocolLength) {
        this.kind = Kind.INCOMING;
        this.sid = sid;
        this.subject = subject;
        if (replyTo != null) {
            this.replyTo = replyTo;
        }
        this.protocolLineLength = protocolLength;
        this.data = null; // will set data and size after we read it
    }

    boolean isProtocol() {
        return this.subject == null;
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
            if (hpub) {
                sizeInBytes += hdrLen + 2; // CRLF
            }
            if (dataLen > 0) {
                sizeInBytes += dataLen + 2; // CRLF
            }
        }
        return sizeInBytes;
    }

    public String getSID() {
        return this.sid;
    }

    // Only for incoming messages, with no protocol bytes
    void setHeaders(byte[] headersBytes) {
        this.headers = new Headers(headersBytes);  // constructor accounts for null and empty
        hdrLen = headersBytes.length;
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
        return this.data;
    }

    public Subscription getSubscription() {
        return this.subscription;
    }

    @Override
    public byte[] getHeadersBytes() {
        return headers == null || headers.size() == 0 ? null : headers.getSerialized();
    }

    @Override
    public Headers getHeaders() {
        return headers;
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
                "\n  hpub=" + hpub +
                "\n  sizeInBytes=" + sizeInBytes +
                "\n  hdrLen=" + hdrLen +
                "\n  dataLen=" + dataLen +
                "\n  totLen=" + totLen +
                "\n  subscription=" + subscription +
                "\n  next=" + next +
                "\n  >" +
                new String(getProtocolBytes()) + "++" + hdrString +
                (data == null ? (kind == Kind.PROTOCOL ? "" : "++") : new String(data, US_ASCII) + "++");
    }
}
