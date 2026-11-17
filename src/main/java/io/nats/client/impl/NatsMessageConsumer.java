// Copyright 2020-2026 The NATS Authors
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
import java.util.concurrent.locks.ReentrantLock;

class NatsMessageConsumer extends NatsMessageConsumerBase implements PullManagerObserver {
    protected final ConsumeOptions consumeOpts;
    protected final SimplifiedSubscriptionMaker subscriptionMaker;
    protected final Dispatcher userDispatcher;
    protected final MessageHandler userMessageHandler;

    protected final int thresholdMessages;
    protected final long thresholdBytes;
    protected final boolean isTrackingBytes;

    // The pending counters are written from the reader thread (a message or a status arriving) and from the
    // dispatcher or user thread (a message handled). Every entry point that touches them takes this lock, so a
    // re-pull issued after a status on the reader thread cannot lose its add to a handler decrement on the other.
    // Subscribing is never done under it: for an ordered consumer that is a JetStream API request whose reply
    // only the reader thread can deliver, and the reader may be waiting for this lock.
    protected final ReentrantLock pendingLock;
    protected int pendingReceivedMessages;
    protected long pendingReceivedBytes;
    protected boolean noReceivedArePending;
    protected boolean forcePull;
    protected int pendingProcessedMessages;
    protected long pendingProcessedBytes;
    protected boolean noProcessedArePending;
    protected boolean processedHasCrossedThreshold;

    NatsMessageConsumer(SimplifiedSubscriptionMaker subscriptionMaker,
                        ConsumerInfo cachedConsumerInfo,
                        ConsumeOptions consumeOpts,
                        Dispatcher userDispatcher,
                        final MessageHandler userMessageHandler) throws IOException, JetStreamApiException
    {
        super(cachedConsumerInfo);

        this.subscriptionMaker = subscriptionMaker;
        this.consumeOpts = consumeOpts;
        this.userDispatcher = userDispatcher;
        this.userMessageHandler = userMessageHandler;

        int bm = consumeOpts.getBatchSize();
        long bb = consumeOpts.getBatchBytes();
        int rePullMessages = Math.max(1, bm * consumeOpts.getThresholdPercent() / 100);
        long rePullBytes = bb == 0 ? 0 : Math.max(1, bb * consumeOpts.getThresholdPercent() / 100);
        // The threshold is what the pending count has to drop below for the next pull. A batch of 1
        // gives a re-pull size of 1 and so a threshold of 0, which pending can never go below, so the
        // consumer pulled once and then stopped. Never let the threshold fall under 1.
        int thresholdM = bm - rePullMessages;
        thresholdMessages = Math.max(thresholdM, 1);
        long thresholdB = bb - rePullBytes;
        thresholdBytes = bb == 0 ? Integer.MIN_VALUE : (thresholdB < 1 ? 1 : thresholdB);
        isTrackingBytes = rePullBytes > 0;
        pendingLock = new ReentrantLock();
        doSub(true);
    }

    protected void fullResetPending() {
        pendingReceivedMessages = 0;
        pendingReceivedBytes = 0;
        noReceivedArePending = true;
        forcePull = true;
        pendingProcessedMessages = 0;
        pendingProcessedBytes = 0;
        noProcessedArePending = true;
        processedHasCrossedThreshold = true;
    }

    protected void statusAdjustPending(int messages, long bytes) {
        pendingReceivedMessages = Math.max(0, pendingReceivedMessages - messages);
        pendingReceivedBytes = Math.max(0, pendingReceivedBytes - bytes);
        noReceivedArePending = pendingReceivedMessages == 0 || (isTrackingBytes && pendingReceivedBytes == 0);
        forcePull = true;
        pendingProcessedMessages = Math.max(0, pendingProcessedMessages - messages);
        pendingProcessedBytes = Math.max(0, pendingProcessedBytes - bytes);
        noProcessedArePending = pendingProcessedMessages == 0 || (isTrackingBytes && pendingProcessedBytes == 0);
        processedHasCrossedThreshold = pendingProcessedMessages < thresholdMessages || (isTrackingBytes && pendingProcessedBytes < thresholdBytes);
    }

    // The outstanding pull is over without the server saying how much of it was undelivered, so the
    // remainder is whatever has not arrived. What has arrived stays queued for the handler, and the
    // processed side keeps counting it, where a full reset would drop it and close with it unhandled.
    protected void remainderAdjustPending() {
        statusAdjustPending(pendingReceivedMessages, pendingReceivedBytes);
    }

    protected void aboutToPull(int messages, long bytes) {
        pendingReceivedMessages += messages;
        pendingReceivedBytes += bytes;
        noReceivedArePending = false;
        forcePull = false;
        pendingProcessedMessages += messages;
        pendingProcessedBytes += bytes;
        noProcessedArePending = false;
        processedHasCrossedThreshold = false;
    }

    protected void updateProcessed(Message msg) {
        pendingLock.lock();
        try {
            pendingProcessedMessages = Math.max(0, pendingProcessedMessages - 1);
            if (pendingProcessedMessages < thresholdMessages) {
                processedHasCrossedThreshold = true;
            }
            if (pendingProcessedMessages == 0) {
                noProcessedArePending = true;
            }
            if (isTrackingBytes) {
                pendingProcessedBytes = Math.max(0, pendingProcessedBytes - msg.consumeByteCount());
                processedHasCrossedThreshold |= pendingProcessedBytes < thresholdBytes;
                noProcessedArePending |= pendingProcessedBytes == 0;
            }
            afterPendingUpdated();
        }
        finally {
            pendingLock.unlock();
        }
    }

    @Override
    public void messageReceived(Message msg) {
        pendingLock.lock();
        try {
            pendingReceivedMessages = Math.max(0, pendingReceivedMessages - 1);
            if (pendingReceivedMessages == 0) {
                noReceivedArePending = true;
            }
            if (isTrackingBytes) {
                pendingReceivedBytes = Math.max(0, pendingReceivedBytes - msg.consumeByteCount());
                noReceivedArePending |= pendingReceivedBytes == 0;
            }
            afterPendingUpdated();
        }
        finally {
            pendingLock.unlock();
        }
    }

    @Override
    public void pullCompletedWithStatus(int messages, long bytes) {
        pendingLock.lock();
        try {
            if (messages == -1) {
                // status without Nats-Pending-* headers
                remainderAdjustPending();
            }
            else {
                statusAdjustPending(messages, bytes);
            }
            afterPendingUpdated();
        }
        finally {
            pendingLock.unlock();
        }
    }

    // Called by the heartbeat alarm, and by the ordered manager when the consumer sequence has a gap.
    @Override
    public void pullTerminatedByError() {
        if (stopped.get()) {
            // The pull is broken, so a stopped consumer closes here. After a sequence gap the ordered
            // manager drops what is queued behind the gap without it being counted as processed, so
            // waiting on the processed side would never finish. On a heartbeat alarm this strands what
            // is still queued, which takes the server silent and no message handled for the whole alarm
            // period after the stop, since the first pending update after stop shuts the timer down.
            fullClose();
        }
        else {
            try {
                shutdownSub();
                doSub(false);
            }
            catch (JetStreamApiException | IOException e) {
                resetOnException();
            }
        }
    }

    void doSub(boolean first) throws JetStreamApiException, IOException {
        MessageHandler mh = userMessageHandler == null ? null : msg -> {
            try {
                userMessageHandler.onMessage(msg);
            }
            finally {
                updateProcessed(msg);
            }
        };

        try {
            stopped.set(false);
            finished.set(false);
            // not under pendingLock, see the comment on the lock
            NatsJetStreamPullSubscription newSub = subscriptionMaker.subscribe(mh, userDispatcher, pmm, null);
            pendingLock.lock();
            try {
                super.initSub(newSub, !first);
                fullResetPending();
                rePull();
            }
            finally {
                pendingLock.unlock();
            }
        }
        catch (JetStreamApiException | IOException e) {
            if (first) {
                throw e;
            }
            resetOnException();
        }
    }

    private void resetOnException() {
        pendingLock.lock();
        try {
            fullResetPending();
        }
        finally {
            pendingLock.unlock();
        }
        pmm.updateLastMessageReceived();
        pmm.initOrResetHeartbeatTimer();
    }

    protected void afterPendingUpdated() {
        if (stopped.get()) {
            // The alarm guards the outstanding pull: if the server goes away before the pull ends, the
            // alarm is what closes the consumer, so the timer stays until nothing is outstanding. After
            // that the server has nothing more to send, and the timer is shut down so the alarm cannot
            // cut the handling of what is queued short.
            if (noReceivedArePending) {
                pmm.shutdownHeartbeatTimer();
            }
            // No received pending means every message of the outstanding pulls has arrived at the
            // client, but some may still be queued for the handler or for nextMessage. fullClose()
            // unsubscribes, and the dispatcher skips queued messages of an inactive subscription, so
            // closing on received alone strands them. No processed pending means every received
            // message has been handled. Both are judged the way a pull completes: by message count,
            // or by byte count when tracking bytes, since a byte-limited pull can fill exactly by
            // bytes and never bring the message count to zero. Finish only when both are done.
            if (noReceivedArePending && noProcessedArePending) {
                fullClose();
            }
        }
        else if (forcePull || processedHasCrossedThreshold) {
            rePull();
        }
    }

    protected void rePull() {
        int rePullMessages = Math.max(1, consumeOpts.getBatchSize() - pendingProcessedMessages);
        long rePullBytes = consumeOpts.getBatchBytes() == 0 ? 0 : consumeOpts.getBatchBytes() - pendingProcessedBytes;
        PinnablePullRequestOptions pro = new PinnablePullRequestOptions(pmm.currentPinId,
            PullRequestOptions.builder(rePullMessages)
                .maxBytes(rePullBytes)
                .expiresIn(consumeOpts.getExpiresInMillis())
                .idleHeartbeat(consumeOpts.getIdleHeartbeat())
                .group(consumeOpts.getGroup())
                .priority(consumeOpts.getPriority())
                .minPending(consumeOpts.getMinPending())
                .minAckPending(consumeOpts.getMinAckPending()));
        aboutToPull(rePullMessages, rePullBytes);
        sub._pull(pro, consumeOpts.raiseStatusWarnings(), this);
    }
}
