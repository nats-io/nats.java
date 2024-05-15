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

import io.nats.client.support.ByteArrayBuilder;

import static io.nats.client.support.Validator.validateReplyTo;
import static io.nats.client.support.Validator.validateSubject;

class NatsPublishableMessage extends NatsMessage {
    final boolean hasHeaders;

    public NatsPublishableMessage(boolean hasHeaders) {
        this.hasHeaders = hasHeaders;
    }

    public NatsPublishableMessage(String subject, String replyTo, Headers headers, byte[] data, boolean validateSubjectAndReply) {
        super(data);
        this.subject = validateSubjectAndReply ? validateSubject(subject, true) : subject;
        this.replyTo = validateSubjectAndReply ? validateReplyTo(replyTo, false) : replyTo;
        if (headers == null || headers.isEmpty()) {
            hasHeaders = false;
        }
        else {
            hasHeaders = true;
            headers = headers.isReadOnly() ? headers : new Headers(headers, true, null);
        }
        this.headers = new Headers(headers, false, null);
        calculate();
    }

    @Override
    ByteArrayBuilder getProtocolBab() {
        // compared to base class, skips calling calculate()
        return protocolBab;
    }

    @Override
    long getSizeInBytes() {
        // compared to base class, skips calling calculate()
        return sizeInBytes;
    }

    @Override
    byte[] getProtocolBytes() {
        // compared to base class, skips calling calculate()
        return protocolBab.toByteArray();
    }

    @Override
    int getControlLineLength() {
        // compared to base class, skips calling calculate()
        return controlLineLength;
    }

    /**
     * @param destPosition the position index in destination byte array to start
     * @param dest is the byte array to write to
     * @return the length of the header
     */
    @Override
    int copyNotEmptyHeaders(int destPosition, byte[] dest) {
        // compared to base class, skips calling calculate()
        if (headerLen > 0) {
            return headers.serializeToArray(destPosition, dest);
        }
        return 0;
    }
}
