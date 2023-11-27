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
    protected final ConsumeOptions opts;
    protected final int thresholdMessages;
    protected final long thresholdBytes;
    protected final SimplifiedSubscriptionMaker subscriptionMaker;
    protected final Dispatcher userDispatcher;
    protected final MessageHandler userMessageHandler;

    NatsMessageConsumer(SimplifiedSubscriptionMaker subscriptionMaker,
                        ConsumerInfo cachedConsumerInfo,
                        ConsumeOptions opts,
                        Dispatcher userDispatcher,
                        final MessageHandler userMessageHandler) throws IOException, JetStreamApiException
    {
        super(cachedConsumerInfo);

        this.subscriptionMaker = subscriptionMaker;
        this.opts = opts;
        this.userDispatcher = userDispatcher;
        this.userMessageHandler = userMessageHandler;

        int bm = opts.getBatchSize();
        long bb = opts.getBatchBytes();
        int rePullMessages = Math.max(1, bm * opts.getThresholdPercent() / 100);
        long rePullBytes = bb == 0 ? 0 : Math.max(1, bb * opts.getThresholdPercent() / 100);
        thresholdMessages = bm - rePullMessages;
        thresholdBytes = bb == 0 ? Integer.MIN_VALUE : bb - rePullBytes;

        doSub();
    }

    @Override
    public void heartbeatError() {
        restart();
    }

    private void restart() {
        try {
            // just close the current sub and make another one.
            // this could go on endlessly
            lenientClose();
            doSub();
        }
        catch (JetStreamApiException | IOException e) {
            setupHbAlarmToTrigger();
        }
    }

    void doSub() throws JetStreamApiException, IOException {
        MessageHandler mh = userMessageHandler == null ? null : msg -> {
            userMessageHandler.onMessage(msg);
            if (stopped.get() && pmm.noMorePending()) {
                finished.set(true);
            }
        };
        try {
            super.initSub(subscriptionMaker.subscribe(mh, userDispatcher, pmm));
            repull();
            stopped.set(false);
        }
        catch (JetStreamApiException | IOException e) {
            setupHbAlarmToTrigger();
        }
    }

    private void setupHbAlarmToTrigger() {
        pmm.resetTracking();
        pmm.initOrResetHeartbeatTimer();
    }

    @Override
    public void pendingUpdated() {
        if (!stopped.get() && (pmm.pendingMessages <= thresholdMessages || (pmm.trackingBytes && pmm.pendingBytes <= thresholdBytes)))
        {
            repull();
        }
    }

    private void repull() {
        int rePullMessages = Math.max(1, opts.getBatchSize() - pmm.pendingMessages);
        long rePullBytes = opts.getBatchBytes() == 0 ? 0 : opts.getBatchBytes() - pmm.pendingBytes;
        PullRequestOptions pro = PullRequestOptions.builder(rePullMessages)
            .maxBytes(rePullBytes)
            .expiresIn(opts.getExpiresInMillis())
            .idleHeartbeat(opts.getIdleHeartbeat())
            .build();
        sub._pull(pro, false, this);
    }
}
