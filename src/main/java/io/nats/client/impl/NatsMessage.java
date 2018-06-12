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

    private String subject;
    private String replyTo;
    private byte[] data;
    private Subscription subscription;
    private byte[] protocolBytes;
    
    long size;
    NatsMessage next; // for linked list
    NatsMessage prev; // for linked list

    NatsMessage(String subject, String replyTo, byte[] data) {
        this(null, subject, replyTo, data);
    }

    NatsMessage(Subscription subscription, String subject, String replyTo, byte[] data) {
        StringBuilder protocolStringBuilder = new StringBuilder();
        this.subject = subject;
        this.replyTo = replyTo;
        this.data = data;
        this.subscription = subscription;

        protocolStringBuilder.append("PUB");
        protocolStringBuilder.append(" ");
        protocolStringBuilder.append(subject);
        protocolStringBuilder.append(" ");

        if (replyTo != null) {
            protocolStringBuilder.append(replyTo);
            protocolStringBuilder.append(" ");
        }

        protocolStringBuilder.append(String.valueOf(data.length));

        this.protocolBytes = protocolStringBuilder.toString().getBytes(StandardCharsets.UTF_8);
        
        this.size = this.protocolBytes.length + data.length + 4;//for 2x \r\n

        // TODO(sasbury): handle invalid protocol strings (too long)
    }

    NatsMessage(String protocol) {
        this.protocolBytes = protocol.getBytes(StandardCharsets.UTF_8);
        this.size = this.protocolBytes.length + 2;//for \r\n
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

	public String getSubject()
	{
		return this.subject;
	}

	public String getReplyTo()
	{
		return this.replyTo;
	}

	public byte[] getData()
	{
		return this.data;
	}

	public Subscription getSubscription()
	{
		return this.subscription;
	}
}