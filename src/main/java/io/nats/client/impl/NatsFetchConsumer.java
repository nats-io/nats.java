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
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerInfo;

import java.io.IOException;

import static io.nats.client.BaseConsumeOptions.MIN_EXPIRES_MILLS;
import static io.nats.client.support.NatsConstants.NANOS_PER_MILLI;

class NatsFetchConsumer extends NatsMessageConsumerBase implements FetchConsumer {
    private final boolean isNoWaitNoExpires;
    private final long maxWaitNanos;
    private final String pullSubject;
    private long startNanos;
    private final boolean isTrackingBytes;
    private int pendingReceivedMessages;
    private long pendingReceivedBytes;
    private boolean noReceivedArePending;

    NatsFetchConsumer(SimplifiedSubscriptionMaker subscriptionMaker,
                      ConsumerInfo cachedConsumerInfo,
                      FetchConsumeOptions fetchConsumeOptions) throws IOException, JetStreamApiException
    {
        super(cachedConsumerInfo);

        boolean isNoWait = fetchConsumeOptions.isNoWait();
        long expiresInMillis = fetchConsumeOptions.getExpiresInMillis();
        isNoWaitNoExpires = isNoWait && expiresInMillis == ConsumerConfiguration.LONG_UNSET;

        long inactiveThreshold;
        if (expiresInMillis == ConsumerConfiguration.LONG_UNSET) { // can be for noWait
            maxWaitNanos = MIN_EXPIRES_MILLS * NANOS_PER_MILLI;
            inactiveThreshold = MIN_EXPIRES_MILLS; // no need to do the 10% longer
        }
        else {
            maxWaitNanos = expiresInMillis * NANOS_PER_MILLI;
            inactiveThreshold = expiresInMillis * 110 / 100; // 10% longer than the wait
        }

        pendingReceivedMessages = fetchConsumeOptions.getMaxMessages();
        pendingReceivedBytes = fetchConsumeOptions.getMaxBytes();
        noReceivedArePending = false;

        isTrackingBytes = pendingReceivedBytes > 0;

        PinnablePullRequestOptions pro = new PinnablePullRequestOptions(pmm == null ? null : pmm.currentPinId,
            PullRequestOptions.builder(pendingReceivedMessages)
                .maxBytes(pendingReceivedBytes)
                .expiresIn(expiresInMillis)
                .idleHeartbeat(fetchConsumeOptions.getIdleHeartbeat())
                .noWait(isNoWait)
                .group(fetchConsumeOptions.getGroup())
                .priority(fetchConsumeOptions.getPriority())
                .minPending(fetchConsumeOptions.getMinPending())
                .minAckPending(fetchConsumeOptions.getMinAckPending()));
        initSub(subscriptionMaker.subscribe(null, null, null, inactiveThreshold), false);
        pullSubject = sub._pull(pro, fetchConsumeOptions.raiseStatusWarnings(), this);
        startNanos = -1;
    }

    @Override
    public void messageReceived(Message msg) {
        pendingReceivedMessages = Math.max(0, pendingReceivedMessages - 1);
        if (pendingReceivedMessages == 0) {
            noReceivedArePending = true;
        }
        else if (isTrackingBytes) {
            pendingReceivedBytes = Math.max(0, pendingReceivedMessages - msg.consumeByteCount());
            noReceivedArePending |= pendingReceivedBytes == 0;
        }
    }

    @Override
    public void pullCompletedWithStatus(int messages, long bytes) {
        pendingReceivedMessages = 0;
        noReceivedArePending = true;
        stopped.set(true);
    }

    @Override
    public void pullTerminatedByError() {
        pendingReceivedMessages = 0;
        noReceivedArePending = true;
        fullClose();
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
            if (noReceivedArePending) {
                Message msg = sub._nextUnmanagedNoWait(pullSubject);
                if (msg == null) {
                    // if there are no messages in the internal cache AND there are no more pending,
                    // they all have been read and we can go ahead and finish
                    fullClose();
                    return null;
                }
                return msg;
            }

            // by not starting the timer until the first call, it gives a little buffer around
            // the next message to account for latency of incoming messages
            if (startNanos == -1) {
                startNanos = NatsSystemClock.nanoTime();
            }
            long timeLeftNanos = maxWaitNanos - (NatsSystemClock.nanoTime() - startNanos);

            // if the timer has run out, don't allow waiting
            // this might happen once, but it should already be noMorePending
            if (timeLeftNanos < NANOS_PER_MILLI) {
                Message msg = sub._nextUnmanagedNoWait(pullSubject);
                if (msg == null) {
                    // no message and no time left, go ahead and finish
                    fullClose();
                }
                return msg;
            }

            Message msg = sub._nextUnmanaged(timeLeftNanos, pullSubject);
            if (msg == null) {
                if (isNoWaitNoExpires) {
                    // no message and no wait, go ahead and finish
                    fullClose();
                }
                return null;
            }
            return msg;
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
