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

import io.nats.client.Message;
import io.nats.client.Subscription;

class NatsMessage implements Message {

    private String subject;
    private String reply;
    private byte[] data;
    private Subscription subscription;
    private String protocolMessage;

    NatsMessage next; // for linked list
    NatsMessage prev; // for linked list

    NatsMessage(String subject, String reply, byte[] data, Subscription subscription) {
        this.subject = subject;
        this.reply = reply;
        this.data = data;
        this.subscription = subscription;
    }

    NatsMessage(String protocol) {
        this.protocolMessage = protocol;
    }

    boolean isProtocol() {
        return this.protocolMessage != null;
    }

    String getProtocolMessage() {
        return this.protocolMessage;
    }

	public String getSubject()
	{
		return this.subject;
	}

	public String getReply()
	{
		return this.reply;
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