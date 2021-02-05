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
import io.nats.client.MessageMetaData;
import io.nats.client.impl.NatsMessage.SelfCalculatingMessage;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import static io.nats.client.impl.NatsJetStreamMessage.AckType.*;
import static io.nats.client.support.Validator.validateDurationRequired;

class NatsJetStreamMessage extends SelfCalculatingMessage {

    enum AckType {
        // Acknowledgement protocol messages
        AckAck("+ACK"),
        AckNak("-NAK"),
        AckProgress("+WPI"),

        // special case
        AckNext("+NXT"),
        AckTerm("+TERM"),
        AckNextOne("+NXT {\"batch\":1}");

        final byte[] body;

        AckType(String body) {
            this.body = body.getBytes();
        }
    }

    private NatsJetStreamMetaData jsMetaData = null;

    public static boolean isJetStream(String replyTo) {
        return replyTo != null && replyTo.startsWith("$JS");
    }

    NatsJetStreamMessage() {}

    @Override
    public void ack() {
        ackReply(AckAck);
    }

    @Override
    public void ackSync(Duration d) throws InterruptedException, TimeoutException {
        ackReply(AckAck, validateDurationRequired(d));
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
    public MessageMetaData metaData() {
        if (this.jsMetaData == null) {
            this.jsMetaData = new NatsJetStreamMetaData(this);
        }
        return this.jsMetaData;
    }

    @Override
    public boolean isJetStream() {
        return true;
    }

    private void ackReply(AckType ackType) {
        try {
            ackReply(ackType, null);
        } catch (InterruptedException e) {
            // we should never get here, but satisfy the linters.
            Thread.currentThread().interrupt();
        } catch (TimeoutException e) {
            // NOOP
        }
    }

    private void ackReply(AckType ackType, Duration dur) throws InterruptedException, TimeoutException {

        // all calls to this must pre-validate the duration
        // this is internal only code and makes this assumption
        boolean isSync = (dur != null);

        Connection nc = getJetStreamValidatedConnection();

        if (isPullMode()) {
            switch (ackType) {
                case AckAck:
                    nc.publish(replyTo, subscription.getSubject(), AckNext.body);
                    break;

                case AckNak:
                    nc.publish(replyTo, subscription.getSubject(), AckNextOne.body);
                    break;

                case AckTerm:
                    nc.publish(replyTo, subscription.getSubject(), AckNextOne.body);
                    break;
            }
            if (isSync && nc.request(replyTo, null, dur) == null) {
                throw new TimeoutException("Ack request next timed out.");
            }
        }
        else if (isSync) {
            if (nc.request(replyTo, ackType.body, dur) == null) {
                throw new TimeoutException("Ack response timed out.");
            }
        }
        else {
            nc.publish(replyTo, ackType.body);
        }
    }

    private boolean isPullMode() {
        return subscription instanceof NatsJetStreamSubscription
                && (((NatsJetStreamSubscription) subscription).defaultBatchSize > 0);
    }

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

    static IllegalStateException notAJetStreamMessage() {
        return new IllegalStateException("Message is not a JetStream message");
    }
}
