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

class NatsMessageConsumer extends NatsMessageConsumerBase implements PullManagerObserver {
    protected final ConsumeOptions consumeOpts;
    protected final SimplifiedSubscriptionMaker subscriptionMaker;
    protected final Dispatcher userDispatcher;
    protected final MessageHandler userMessageHandler;

    protected final int thresholdMessages;
    protected final long thresholdBytes;
    protected final boolean isTrackingBytes;

    protected int pendingReceivedMessages;
    protected long pendingReceivedBytes;
    protected boolean noReceivedArePending;
    protected boolean forcePull;
    protected int pendingProcessedMessages;
    protected long pendingProcessedBytes;
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
        thresholdMessages = bm - rePullMessages;
        thresholdBytes = bb == 0 ? Integer.MIN_VALUE : bb - rePullBytes;
        isTrackingBytes = rePullBytes > 0;
        doSub(true);
    }

    protected void fullResetPending() {
        pendingReceivedMessages = 0;
        pendingReceivedBytes = 0;
        noReceivedArePending = true;
        forcePull = true;
        pendingProcessedMessages = 0;
        pendingProcessedBytes = 0;
        processedHasCrossedThreshold = true;
    }

    protected void statusAdjustPending(int messages, long bytes) {
        pendingReceivedMessages = Math.max(0, pendingReceivedMessages - messages);
        pendingReceivedBytes = Math.max(0, pendingReceivedBytes - bytes);
        noReceivedArePending = pendingReceivedMessages == 0 || (isTrackingBytes && pendingReceivedBytes == 0);
        forcePull = true;
        pendingProcessedMessages = Math.max(0, pendingProcessedMessages - messages);
        pendingProcessedBytes = Math.max(0, pendingProcessedBytes - bytes);
        processedHasCrossedThreshold = pendingProcessedMessages < thresholdMessages || (isTrackingBytes && pendingProcessedBytes < thresholdBytes);
    }

    protected void aboutToPull(int messages, long bytes) {
        pendingReceivedMessages += messages;
        pendingReceivedBytes += bytes;
        noReceivedArePending = false;
        forcePull = false;
        pendingProcessedMessages += messages;
        pendingProcessedBytes += bytes;
        processedHasCrossedThreshold = false;
    }

    protected void updateProcessed(Message msg) {
        pendingProcessedMessages = Math.max(0, pendingProcessedMessages - 1);
        if (pendingProcessedMessages < thresholdMessages) {
            processedHasCrossedThreshold = true;
        }
        if (isTrackingBytes) {
            pendingProcessedBytes = Math.max(0, pendingProcessedBytes - msg.consumeByteCount());
            processedHasCrossedThreshold |= pendingProcessedBytes < thresholdBytes;
        }
        afterPendingUpdated();
    }

    @Override
    public void messageReceived(Message msg) {
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

    @Override
    public void pullCompletedWithStatus(int messages, long bytes) {
        if (messages == -1) {
            // status without Nats-Pending-* headers
            fullResetPending();
        }
        else {
            statusAdjustPending(messages, bytes);
        }
        afterPendingUpdated();
    }

    @Override
    public void pullTerminatedByError() {
        if (stopped.get()) {
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
            super.initSub(subscriptionMaker.subscribe(mh, userDispatcher, pmm, null), !first);
            fullResetPending();
            rePull();
        }
        catch (JetStreamApiException | IOException e) {
            resetOnException();
        }
    }

    private void resetOnException() {
        fullResetPending();
        pmm.updateLastMessageReceived();
        pmm.initOrResetHeartbeatTimer();
    }

    protected void afterPendingUpdated() {
        if (stopped.get()) {
            pmm.shutdownHeartbeatTimer();
            if (noReceivedArePending) {
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
