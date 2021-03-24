// Copyright 2020 The NATS Authors
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
import io.nats.client.impl.NatsMessage.InternalMessage;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import static io.nats.client.impl.AckType.*;
import static io.nats.client.support.Validator.validateDurationRequired;

class NatsJetStreamMessage extends InternalMessage {

    private NatsJetStreamMetaData jsMetaData = null;

    NatsJetStreamMessage() {}

    @Override
    public void ack() {
        ackReply(AckAck);
    }

    @Override
    public void ackSync(Duration d) throws InterruptedException, TimeoutException {
        ackReplySync(AckAck, validateDurationRequired(d));
    }

    @Override
    public void nak() {
        ackReply(AckNak);
    }

    @Override
    public void inProgress() {
        ackReply(AckProgress);
    }

    @Override
    public void term() {
        ackReply(AckTerm);
    }

    @Override
    public NatsJetStreamMetaData metaData() {
        if (this.jsMetaData == null) {
            this.jsMetaData = new NatsJetStreamMetaData(this);
        }
        return this.jsMetaData;
    }

    @Override
    public boolean isJetStream() {
        return true; // NatsJetStreamMessage will never be created unless it's actually a JetStream Message
    }

    private void ackReply(AckType ackType) {
        Connection nc = getJetStreamValidatedConnection();
// SFF 2021-02-25 Future ackNext() behavior
//        if (ackType == AckNext) {
//            byte[] bytes = ((NatsJetStreamSubscription) subscription).getPrefixedPullJson(AckNext.text);
//            nc.publish(replyTo, bytes);
//        }
//        else {
//            nc.publish(replyTo, ackType.bytes);
//        }
        nc.publish(replyTo, ackType.bytes);
    }

    private void ackReplySync(AckType ackType, Duration dur) throws InterruptedException, TimeoutException {
        Connection nc = getJetStreamValidatedConnection();
        if (nc.request(replyTo, ackType.bytes, dur) == null) {
            throw new TimeoutException("Ack response timed out.");
        }
    }

// SFF 2021-02-25 Future ackNext() behavior
//    private boolean isPullMode() {
//        return subscription instanceof NatsJetStreamSubscription
//                && ((NatsJetStreamSubscription) subscription).isPullMode();
//    }

    private Connection getJetStreamValidatedConnection() {
        if (getSubscription() == null) {
            throw new IllegalStateException("Message is not bound to a subscription.");
        }

        Connection c = getConnection();
        if (c == null) {
            throw new IllegalStateException("Message is not bound to a connection");
        }
        return c;
    }
}
