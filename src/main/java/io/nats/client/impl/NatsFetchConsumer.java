// Copyright 2020-2023 The NATS Authors
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

import io.nats.client.*;

class NatsFetchConsumer extends NatsSimpleConsumerBase implements FetchConsumer {
    private final long maxWaitNanos;
    private long start;

    public NatsFetchConsumer(NatsConsumerContext.SubscriptionMaker subscriptionMaker, FetchConsumeOptions consumeOptions) {
        initSub(subscriptionMaker.makeSubscription(null));
        maxWaitNanos = consumeOptions.getExpiresIn() * 1_000_000;
        sub._pull(PullRequestOptions.builder(consumeOptions.getMaxMessages())
            .maxBytes(consumeOptions.getMaxBytes())
            .expiresIn(consumeOptions.getExpiresIn())
            .idleHeartbeat(consumeOptions.getIdleHeartbeat())
            .build()
        );
        start = -1;
    }

    @Override
    public Message nextMessage() throws InterruptedException, JetStreamStatusCheckedException {
        try {
            if (start == -1) {
                start = System.nanoTime();
            }

            if (pmm.pendingMessages < 1 || (pmm.trackingBytes && pmm.pendingBytes < 1)) {
                // nothing pending means the client has already received all it is going to
                // null for nextMessage means don't wait, the queue either has something already
                // or there aren't any messages left
                return sub._nextUnmanagedNullOrLteZero(null); // null means don't wait
            }

            long timeLeftMillis = (maxWaitNanos - (System.nanoTime() - start)) / 1_000_000;
            if (timeLeftMillis < 1) {
                return sub._nextUnmanagedNullOrLteZero(null); // null means don't wait
            }

            return sub.nextMessage(timeLeftMillis);
        }
        catch (InterruptedException r) {
            stopInternal();
            throw r;
        }
        catch (JetStreamStatusException e) {
            stopInternal();
            throw new JetStreamStatusCheckedException(e);
        }
    }
}
