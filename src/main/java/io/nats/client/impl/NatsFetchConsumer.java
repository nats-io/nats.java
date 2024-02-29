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
import io.nats.client.api.ConsumerInfo;

import java.io.IOException;

class NatsFetchConsumer extends NatsMessageConsumerBase implements FetchConsumer, PullManagerObserver {
    private final boolean isNoWait;
    private final long maxWaitNanos;
    private final String pullSubject;
    private long startNanos;

    NatsFetchConsumer(SimplifiedSubscriptionMaker subscriptionMaker,
                      ConsumerInfo cachedConsumerInfo,
                      FetchConsumeOptions fetchConsumeOptions) throws IOException, JetStreamApiException
    {
        super(cachedConsumerInfo);

        isNoWait = fetchConsumeOptions.isNoWait();
        long expiresInMillis = fetchConsumeOptions.getExpiresInMillis();
        long inactiveThreshold;
        if (expiresInMillis == 0) { // can be for noWait
            maxWaitNanos = 100_000_000; // 100ms
            inactiveThreshold = 1000;
        }
        else {
            maxWaitNanos = expiresInMillis * 1_000_000;
            inactiveThreshold = expiresInMillis * 110 / 100; // ten % longer than the wait
        }
        PullRequestOptions pro = PullRequestOptions.builder(fetchConsumeOptions.getMaxMessages())
            .maxBytes(fetchConsumeOptions.getMaxBytes())
            .expiresIn(expiresInMillis)
            .idleHeartbeat(fetchConsumeOptions.getIdleHeartbeat())
            .noWait(isNoWait)
            .build();
        initSub(subscriptionMaker.subscribe(null, null, null, inactiveThreshold));
        pullSubject = sub._pull(pro, false, this);
        startNanos = -1;
    }

    @Override
    public void pendingUpdated() {}

    @Override
    public void heartbeatError() {
        stopped.set(true);
        finished.set(true);
    }

    @Override
    public Message nextMessage() throws InterruptedException, JetStreamStatusCheckedException {
        try {
            if (finished.get()) {
                return null;
            }

            // if the manager thinks it has received everything in the pull, it means
            // that all the messages are already in the internal queue and there is
            // no waiting necessary
            if (pmm.noMorePending()) {
                Message m = sub._nextUnmanagedNoWait(pullSubject);
                if (m == null) {
                    // if there are no messages in the internal cache AND there are no more pending,
                    // they all have been read and we can go ahead and close the subscription.
                    finished.set(true);
                    lenientClose();
                }
                return m;
            }

            // by not starting the timer until the first call, it gives a little buffer around
            // the next message to account for latency of incoming messages
            if (startNanos == -1) {
                startNanos = System.nanoTime();
            }
            long timeLeftMillis = (maxWaitNanos - (System.nanoTime() - startNanos)) / 1_000_000;

            // if the timer has run out, don't allow waiting
            // this might happen once, but it should already be noMorePending
            if (timeLeftMillis < 1) {
                Message m = sub._nextUnmanagedNoWait(pullSubject); // null means don't wait
                if (m == null && isNoWait) {
                    finished.set(true);
                    lenientClose();
                }
                return m;
            }

            return sub._nextUnmanaged(timeLeftMillis, pullSubject);
        }
        catch (JetStreamStatusException e) {
            throw new JetStreamStatusCheckedException(e);
        }
        catch (IllegalStateException i) {
            // this happens if the consumer is stopped, since it is
            // drained/unsubscribed, so don't pass it on if it's expected
            return null;
        }
    }
}
