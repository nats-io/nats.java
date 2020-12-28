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

import io.nats.client.Connection;
import io.nats.client.MessageMetaData;
import io.nats.client.impl.NatsMessage.SelfCalculatingMessage;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

class NatsJetStreamMessage extends SelfCalculatingMessage {

    // Acknowedgement protocol messages
    private static final byte[] AckAck = "+ACK".getBytes();
    private static final byte[] AckNak = "-NAK".getBytes();
    private static final byte[] AckProgress = "+WPI".getBytes();

    // special case
    private static final byte[] AckNext = "+NXT".getBytes();
    private static final byte[] AckTerm = "+TERM".getBytes();
    private static final byte[] AckNextOne = "+NXT {\"batch\":1}".getBytes();

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
        ackReply(AckAck, d);
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

    private void ackReply(byte[] ackType) {
        try {
            ackReply(ackType, Duration.ZERO);
        } catch (InterruptedException e) {
            // we should never get here, but satisfy the linters.
            Thread.currentThread().interrupt();
        } catch (TimeoutException e) {
            // NOOP
        }
    }

    private void ackReply(byte[] ackType, Duration d) throws InterruptedException, TimeoutException {
        if (d == null) {
            throw new IllegalArgumentException("Duration cannot be null.");
        }

        boolean isSync = (d != Duration.ZERO);
        Connection nc = getJetStreamValidatedConnection();

        if (isPullMode()) {
            if (Arrays.equals(ackType, AckAck)) {
                nc.publish(replyTo, subscription.getSubject(), AckNext);
            } else if (Arrays.equals(ackType, AckNak) || Arrays.equals(ackType, AckTerm)) {
                nc.publish(replyTo, subscription.getSubject(), AckNextOne);
            }
            if (isSync && nc.request(replyTo, null, d) == null) {
                throw new TimeoutException("Ack request next timed out.");
            }

        } else if (isSync && nc.request(replyTo, ackType, d) == null) {
            throw new TimeoutException("Ack response timed out.");
        } else {
            nc.publish(replyTo, ackType);
        }
    }

    private boolean isPullMode() {
        if (!(this.subscription instanceof NatsJetStreamSubscription)) {
            return false;
        }
        return (((NatsJetStreamSubscription) this.subscription).pull > 0);
    }

    private Connection getJetStreamValidatedConnection() {

        if (getSubscription() == null) {
            throw new IllegalStateException("Messages is not bound to a subscription.");
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
