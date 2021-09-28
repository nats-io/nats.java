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

package io.nats.client;

/**
 * JetStreamGapException is used to indicate that a message gap has been detected
 */
public class JetStreamGapException extends IllegalStateException {
    private final JetStreamSubscription sub;
    private final long expectedConsumerSeq;
    private final long receivedConsumerSeq;

    public JetStreamGapException(JetStreamSubscription sub, long expectedConsumerSeq, long receivedConsumerSeq) {
        this.sub = sub;
        this.expectedConsumerSeq = expectedConsumerSeq;
        this.receivedConsumerSeq = receivedConsumerSeq;
    }

    /**
     * Get the subscription this issue occurred on
     *
     * @return the subscription
     */
    public JetStreamSubscription getSubscription() {
        return sub;
    }

    /**
     * Get the consumer sequence that was expected
     *
     * @return the expected consumer sequence
     */
    public long getExpectedConsumerSeq() {
        return expectedConsumerSeq;
    }

    /**
     * Get the consumer sequence that was received
     *
     * @return the received consumer sequence
     */
    public long getReceivedConsumerSeq() {
        return receivedConsumerSeq;
    }
}
