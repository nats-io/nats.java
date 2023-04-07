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

import java.io.IOException;

import static io.nats.client.impl.NatsConsumerContext.NjsPullSubscriptionMaker;

class NatsFetchConsumer extends NatsConsumerSubscription implements FetchConsumer {
    long start;
    long maxWaitNanos;
    public NatsFetchConsumer(NjsPullSubscriptionMaker subMaker, FetchConsumeOptions consumeOptions) throws IOException, JetStreamApiException {
        setSub(subMaker.makeSubscription());
        sub.pull(PullRequestOptions.builder(consumeOptions.getMaxMessages())
            .maxBytes(consumeOptions.getMaxBytes())
            .expiresIn(consumeOptions.getExpires())
            .idleHeartbeat(consumeOptions.getIdleHeartbeat())
            .build()
        );
        maxWaitNanos = consumeOptions.getExpires() * 1_000_000;
    }

    @Override
    public Message nextMessage() throws InterruptedException {
        if (start == 0) {
            start = System.nanoTime();
        }
        Message m;
        if (pmm.pendingMessages < 1 || (pmm.trackingBytes && pmm.pendingBytes < 1)) {
            // nothing pending means the client has already received all it is going to
            // null for nextMessage means don't wait, the queue either has something already
            // or there aren't any messages left
            m = sub.nextMessage(null);
        }
        else {
            long timeLeftMillis = (maxWaitNanos - (System.nanoTime() - start)) / 1_000_000;
            if (timeLeftMillis < 1) {
                m = sub.nextMessage(null);
            }
            else {
                m = sub.nextMessage(timeLeftMillis);
            }
        }
        if (m == null) {
            // if we are out of messages, unsub, but do it on a
            // separate thread, so we can return to the user w/o waiting
            sub.connection.getExecutor().submit(() -> unsubscribe(-1));
        }
        return m;
    }
}
