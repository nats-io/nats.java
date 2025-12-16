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

class NatsPublishableMessage extends NatsMessage {
    final boolean hasHeaders;

    public NatsPublishableMessage(boolean hasHeaders) {
        this.hasHeaders = hasHeaders;
        flushImmediatelyAfterPublish = false;
    }

    public NatsPublishableMessage(String subject, String replyTo, Headers headers, byte[] data, boolean flushImmediatelyAfterPublish) {
        super(data);
        this.flushImmediatelyAfterPublish = flushImmediatelyAfterPublish;
        this.subject = subject;
        this.replyTo = replyTo;
        if (headers == null || headers.isEmpty()) {
            hasHeaders = false;
        }
        else {
            hasHeaders = true;
            this.headers = headers.isReadOnly() ? headers : new Headers(headers, true, null);
        }
        super.calculate();
    }

    @Override
    protected void calculate() {
        // it's already done in the constructor
    }
}
