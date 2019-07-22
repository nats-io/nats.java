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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import io.nats.client.Message;
import io.nats.client.Subscription;

class NatsMessage implements Message {
    private String sid;
    private String subject;
    private String replyTo;
    private byte[] data;
    private byte[] protocolBytes;
    private NatsSubscription subscription;
    private long sizeInBytes;
    
    NatsMessage next; // for linked list

    static final byte[] digits = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

    static int copy(byte[] dest, int pos, String toCopy) {
        for (int i=0, max=toCopy.length(); i<max ;i++) {
            dest[pos] = (byte) toCopy.charAt(i);
            pos++;
        }

        return pos;
    }

    private static String PUB_SPACE = NatsConnection.OP_PUB + " ";
    private static String SPACE = " ";

    // Create a message to publish
    NatsMessage(String subject, String replyTo, byte[] data, boolean utf8mode) {
        this.subject = subject;
        this.replyTo = replyTo;
        this.data = data;
        
        if (utf8mode) {
            int subjectSize = subject.length() * 2;
            int replySize = (replyTo != null) ? replyTo.length() * 2 : 0;
            StringBuilder protocolStringBuilder = new StringBuilder(4 + subjectSize + 1 + replySize + 1);
            protocolStringBuilder.append(PUB_SPACE);
            protocolStringBuilder.append(subject);
            protocolStringBuilder.append(SPACE);
    
            if (replyTo != null) {
                protocolStringBuilder.append(replyTo);
                protocolStringBuilder.append(SPACE);
            }
    
            protocolStringBuilder.append(String.valueOf(data.length));

            this.protocolBytes = protocolStringBuilder.toString().getBytes(StandardCharsets.UTF_8);
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

            this.protocolBytes = new byte[len];

            // Copy everything
            int pos = 0;
            protocolBytes[0] = 'P';
            protocolBytes[1] = 'U';
            protocolBytes[2] = 'B';
            protocolBytes[3] = ' ';
            pos = 4;
            pos = copy(protocolBytes, pos, subject);
            protocolBytes[pos] = ' ';
            pos++;

            if (replyTo != null) {
                pos = copy(protocolBytes, pos, replyTo);
                protocolBytes[pos] = ' ';
                pos++;
            }

            System.arraycopy(lengthBytes, idx, protocolBytes, pos, lengthBytes.length - idx);
        }

        this.sizeInBytes = this.protocolBytes.length + data.length + 4;// for 2x \r\n
    }

    // Create a protocol only message to publish
    NatsMessage(CharBuffer protocol) {
        ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(protocol);
        this.protocolBytes = Arrays.copyOfRange(byteBuffer.array(), byteBuffer.position(), byteBuffer.limit());
        Arrays.fill(byteBuffer.array(), (byte) 0); // clear sensitive data
        this.sizeInBytes = this.protocolBytes.length + 2;// for \r\n
    }

    // Create an incoming message for a subscriber
    // Doesn't check controlline size, since the server sent us the message
    NatsMessage(String sid, String subject, String replyTo, int protocolLength) {
        this.sid = sid;
        this.subject = subject;
        if (replyTo != null) {
            this.replyTo = replyTo;
        }
        this.sizeInBytes = protocolLength + 2;
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
        return sizeInBytes;
    }

    public String getSID() {
        return this.sid;
    }

    // Only for incoming messages, with no protocol bytes
    void setData(byte[] data) {
        this.data = data;
        this.sizeInBytes += data.length + 2;// for \r\n, we already set the length for the protocol bytes in the constructor
    }

    void setSubscription(NatsSubscription sub) {
        this.subscription = sub;
    }

    NatsSubscription getNatsSubscription() {
        return this.subscription;
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
}
