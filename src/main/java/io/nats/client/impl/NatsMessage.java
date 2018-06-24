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

import java.nio.charset.StandardCharsets;

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

    // Create a message to publish
    NatsMessage(String subject, String replyTo, byte[] data) {
        // Each call to this method takes about 100ns due to the string append
        // possible performance improvement opportunity (perhaps at the cost of readability).
        StringBuilder protocolStringBuilder = new StringBuilder();
        this.subject = subject;
        this.replyTo = replyTo;
        this.data = data;

        protocolStringBuilder.append("PUB ");
        protocolStringBuilder.append(subject);
        protocolStringBuilder.append(" ");

        if (replyTo != null) {
            protocolStringBuilder.append(replyTo);
            protocolStringBuilder.append(" ");
        }

        protocolStringBuilder.append(String.valueOf(data.length));

        String protocol = protocolStringBuilder.toString();
        this.protocolBytes = protocol.getBytes(StandardCharsets.UTF_8);

        this.sizeInBytes = this.protocolBytes.length + data.length + 4;// for 2x \r\n
    }

    // Create a protocol only message to publish
    NatsMessage(String protocol) {
        this.protocolBytes = protocol.getBytes(StandardCharsets.UTF_8);
        this.sizeInBytes = this.protocolBytes.length + 2;// for \r\n
    }

    // Create an incoming message for a subscriber
    // Doesn't check controlline size, since the server sent us the message
    NatsMessage(String sid, String subject, String replyTo, int protocolLength) {
        this.sid = sid;
        this.subject = subject;
        this.replyTo = replyTo;
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

    String getSID() {
        return this.sid;
    }

    public String getSubject() {
        return this.subject;
    }

    public String getReplyTo() {
        return this.replyTo;
    }

    // Only for incoming messages, with no protocol bytes
    void setData(byte[] data) {
        this.data = data;
        this.sizeInBytes += data.length + 2;// for \r\n, we already set the length for the protocol bytes in the constructor
    }

    public byte[] getData() {
        return this.data;
    }

    void setSubscription(NatsSubscription sub) {
        this.subscription = sub;
    }

    public Subscription getSubscription() {
        return this.subscription;
    }

    NatsSubscription getNatsSubscription() {
        return this.subscription;
    }
}