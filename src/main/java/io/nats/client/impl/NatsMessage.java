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

    // TODO(sasbury): Can we slim this down
    private String sid;
    private String subject;
    private String replyTo;
    private byte[] data;
    private byte[] protocolBytes;
    private NatsSubscription subscription;

    long size;
    NatsMessage next; // for linked list
    NatsMessage prev; // for linked list

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

        if (this.protocolBytes.length > NatsConnection.MAX_PROTOCOL_LINE) {
            throw new IllegalArgumentException("Protocol line is too long "+protocol);
        }

        this.size = this.protocolBytes.length + data.length + 4;// for 2x \r\n
    }

    // Create a protocol only message to publish
    NatsMessage(String protocol) {
        this.protocolBytes = protocol.getBytes(StandardCharsets.UTF_8);
        this.size = this.protocolBytes.length + 2;// for \r\n
        if (this.protocolBytes.length > NatsConnection.MAX_PROTOCOL_LINE) {
            throw new IllegalArgumentException("Protocol line is too long "+protocol);
        }
    }

    // Create an incoming message for a subscriber
    NatsMessage(String sid, String subject, String replyTo, String protocol) {
        this.sid = sid;
        this.subject = subject;
        this.replyTo = replyTo;
        this.protocolBytes = protocol.getBytes(StandardCharsets.UTF_8);
        this.data = null; // will set data and size after we read it
    }

    boolean isProtocol() {
        return this.subject == null;
    }

    byte[] getProtocolBytes() {
        return this.protocolBytes;
    }

    long getSize() {
        return size;
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

    void setData(byte[] data) {
        this.data = data;
        this.size = this.protocolBytes.length + data.length + 4;// for 2x \r\n
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