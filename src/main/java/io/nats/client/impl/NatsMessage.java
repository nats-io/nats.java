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

import java.io.ByteArrayOutputStream;
import java.nio.CharBuffer;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

class NatsMessage implements Message {
    private static byte[] PUB_BYTES = "PUB ".getBytes();
    private static byte[] HPUB_BYTES = "HPUB ".getBytes();
    private static byte[] SPACE = " ".getBytes();
    private static byte[] CRLF = "\r\n".getBytes();

    public enum Kind {REGULAR, PROTOCOL, INCOMING}

    // Kind.REGULAR : just these fields
    private String subject;
    private String replyTo;
    private byte[] data;
    private boolean utf8mode;
    private byte[] headersBytes;

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
        this.data = data;
        dataLen = data == null ? 0 : data.length;
        this.utf8mode = utf8mode;
        this.headersBytes = headers == null || headers.size() == 0 ? null : headers.getSerialized();
        hpub = headers != null;

        byte[] proto;
        if (hpub) {
            hdrLen = headers.serializedLength();
            proto = HPUB_BYTES;
        }
        else {
            hdrLen = 0;
            proto = PUB_BYTES;
        }
        totLen = hdrLen + dataLen;

        // start the buffer building with the protocol
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        append(baos, proto);

        // next comes the subject
        append(baos, subject.getBytes(utf8mode ? UTF_8 : US_ASCII));
        append(baos, SPACE);

        // reply to if it's there
        if (replyTo != null && replyTo.length() > 0) {
            append(baos, replyTo.getBytes(US_ASCII));
            append(baos, SPACE);
        }

        // header length if there are headers
        if (hpub) {
            append(baos, Integer.toString(hdrLen).getBytes(US_ASCII));
            append(baos, SPACE);
        }

        // payload length
        append(baos, Integer.toString(totLen).getBytes(US_ASCII));

        protocolBytes = baos.toByteArray();
    }

    private void append(ByteArrayOutputStream baos, byte[] bytes) {
        baos.write(bytes, 0, bytes.length);
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
    void setData(byte[] data) {
        this.data = data;
       dataLen = data.length;
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
    public byte[] getHeaders() {
        return headersBytes;
    }


    @Override
    public String toString() {
        String headers = headersBytes == null ? "" : new String(headersBytes, US_ASCII).replace("\r", "+").replace("\n", "+");
        return "NatsMessage:" +
                "\n  subject='" + subject + '\'' +
                "\n  replyTo='" + replyTo + '\'' +
                "\n  data=" + (data == null ? null : new String(data, UTF_8)) +
                "\n  utf8mode=" + utf8mode +
                "\n  headers=" + headers +
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
                new String(getProtocolBytes()) + "++" + headers +
                (data == null ? (kind == Kind.PROTOCOL ? "" : "++") : new String(data, US_ASCII) + "++");
    }
}
