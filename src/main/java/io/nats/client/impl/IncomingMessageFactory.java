// Copyright 2015-2022 The NATS Authors
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

import io.nats.client.support.IncomingHeadersProcessor;
import io.nats.client.support.Status;

import static io.nats.client.support.NatsJetStreamConstants.JS_ACK_SUBJECT_PREFIX;

// ----------------------------------------------------------------------------------------------------
// Incoming Message Factory - internal use only
// ----------------------------------------------------------------------------------------------------
class IncomingMessageFactory {
    private final String sid;
    private final String subject;
    private final String replyTo;
    private final int protocolLineLength;
    private final boolean utf8mode;

    private byte[] data;
    private Headers headers;
    private Status status;
    private int hdrLen = 0;
    private int dataLen = 0;
    private int totLen = 0;

    // Create an incoming message for a subscriber
    // Doesn't check control line size, since the server sent us the message
    IncomingMessageFactory(String sid, String subject, String replyTo, int protocolLength, boolean utf8mode) {
        this.sid = sid;
        this.subject = subject;
        this.replyTo = replyTo;
        this.protocolLineLength = protocolLength;
        this.utf8mode = utf8mode;
        // headers and data are set later and sizes are calculated during those setters
    }

    void setHeaders(IncomingHeadersProcessor ihp) {
        headers = ihp.getHeaders();
        status = ihp.getStatus();
        hdrLen = ihp.getSerializedLength();
        totLen = hdrLen + dataLen;
    }

    void setData(byte[] data) {
        this.data = data;
        dataLen = data == null ? 0 : data.length;
        totLen = hdrLen + dataLen;
    }

    NatsMessage getMessage() {
        NatsMessage message;
        if (status != null) {
            message = new StatusMessage(status);
        }
        else if (replyTo != null && replyTo.startsWith(JS_ACK_SUBJECT_PREFIX)) {
            message = new NatsJetStreamMessage();
        }
        else {
            message = new IncomingMessage();
        }
        message.initData(data);
        message.sid = this.sid;
        message.subject = this.subject;
        message.replyTo = this.replyTo;
        message.protocolLineLength = this.protocolLineLength;
        message.headers = this.headers;
        message.utf8mode = this.utf8mode;
        message.hdrLen = this.hdrLen;
        message.dataLen = this.dataLen;
        message.sizeInBytes = protocolLineLength + hdrLen + dataLen + 4; // Two CRLFs
        return message;
    }
}
