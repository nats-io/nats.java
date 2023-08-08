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
    protected final PullRequestOptions rePullPro;
    protected final int thresholdMessages;
    protected final long thresholdBytes;

    NatsMessageConsumer(SimplifiedSubscriptionMaker subscriptionMaker,
                        ConsumerInfo cachedConsumerInfo,
                        MessageHandler messageHandler,
                        ConsumeOptions opts) throws IOException, JetStreamApiException {
        super(cachedConsumerInfo);

        int bm = opts.getBatchSize();
        long bb = opts.getBatchBytes();

        int rePullMessages = Math.max(1, bm * opts.getThresholdPercent() / 100);
        long rePullBytes = bb == 0 ? 0 : Math.max(1, bb * opts.getThresholdPercent() / 100);
        rePullPro = PullRequestOptions.builder(rePullMessages)
            .maxBytes(rePullBytes)
            .expiresIn(opts.getExpiresInMillis())
            .idleHeartbeat(opts.getIdleHeartbeat())
            .build();

        thresholdMessages = bm - rePullMessages;
        thresholdBytes = bb == 0 ? Integer.MIN_VALUE : bb - rePullBytes;

        MessageHandler mh = messageHandler == null ? null : new SequenceTracker(messageHandler);
        initSub(subscriptionMaker.subscribe(mh));
        sub._pull(PullRequestOptions.builder(bm)
                .maxBytes(bb)
                .expiresIn(opts.getExpiresInMillis())
                .idleHeartbeat(opts.getIdleHeartbeat())
                .build(),
            false, this);
    }

    @Override
    public void pendingUpdated() {
        if (!stopped && (pmm.pendingMessages <= thresholdMessages || (pmm.trackingBytes && pmm.pendingBytes <= thresholdBytes)))
        {
            sub._pull(rePullPro, false, this);
        }
    }

    class SequenceTracker implements MessageHandler {
        MessageHandler userHandler;

        public SequenceTracker(MessageHandler userHandler) {
            this.userHandler = userHandler;
        }

        @Override
        public void onMessage(Message msg) throws InterruptedException {
            if (stopped && pmm.noMorePending()) {
                finished = true;
            }
            userHandler.onMessage(msg);
        }
    }
}
