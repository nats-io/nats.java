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
    protected final int thresholdMessages;
    protected final long thresholdBytes;
    protected final SimplifiedSubscriptionMaker subscriptionMaker;
    protected final Dispatcher userDispatcher;
    protected final MessageHandler userMessageHandler;

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

        doSub(true);
    }

    @Override
    public void heartbeatError() {
        try {
            if (stopped.get()) {
                fullClose();
            }
            else {
                shutdownSub();
                doSub(false);
            }
        }
        catch (JetStreamApiException | IOException e) {
            setupHbAlarmToTrigger();
        }
    }

    void doSub(boolean first) throws JetStreamApiException, IOException {
        MessageHandler mh = userMessageHandler == null ? null : msg -> {
            try {
                userMessageHandler.onMessage(msg);
            }
            finally {
                updatePending(1, msg.consumeByteCount());
            }
        };

        try {
            stopped.set(false);
            finished.set(false);
            super.initSub(subscriptionMaker.subscribe(mh, userDispatcher, pmm, null), !first);
            rePull();
        }
        catch (JetStreamApiException | IOException e) {
            setupHbAlarmToTrigger();
        }
    }

    protected void afterUpdatePending() {
        if (stopped.get()) {
            if (noMorePending()) {
                fullClose();
            }
        }
        else if (pendingMessages <= thresholdMessages || (isTrackingBytes && pendingBytes <= thresholdBytes)) {
            rePull();
        }
    }

    private void setupHbAlarmToTrigger() {
        pmm.resetTracking();
        pmm.initOrResetHeartbeatTimer();
    }

    protected void rePull() {
        int rePullMessages = Math.max(1, consumeOpts.getBatchSize() - pendingMessages);
        long rePullBytes = consumeOpts.getBatchBytes() == 0 ? 0 : consumeOpts.getBatchBytes() - pendingBytes;
        PinnablePullRequestOptions pro = new PinnablePullRequestOptions(pmm.currentPinId,
            PullRequestOptions.builder(rePullMessages)
                .maxBytes(rePullBytes)
                .expiresIn(consumeOpts.getExpiresInMillis())
                .idleHeartbeat(consumeOpts.getIdleHeartbeat())
                .group(consumeOpts.getGroup())
                .priority(consumeOpts.getPriority())
                .minPending(consumeOpts.getMinPending())
                .minAckPending(consumeOpts.getMinAckPending()));
        sub._pull(pro, consumeOpts.raiseStatusWarnings(), this);
    }
}
