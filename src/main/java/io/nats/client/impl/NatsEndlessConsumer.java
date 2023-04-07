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

public class NatsEndlessConsumer extends NatsConsumerSubscription implements ConsumerSubscription, EndlessConsumer {
    private final PullRequestOptions pro;
    private final int thresholdMessages;
    private final int thresholdBytes;

    public NatsEndlessConsumer(NatsConsumerContext.NjsPullSubscriptionMaker subMaker, ConsumeOptions opts) throws IOException, JetStreamApiException {
        setSub(subMaker.makeSubscription());

        int bm = opts.getBatchSize();
        int bb = opts.getBatchBytes();

        PullRequestOptions firstPro = PullRequestOptions.builder(bm)
            .maxBytes(bb)
            .expiresIn(opts.getExpires())
            .idleHeartbeat(opts.getIdleHeartbeat())
            .build();
        sub.pull(firstPro);

        int repullMessages = Math.max(1, bm * opts.getThresholdPercent() / 100);
        int repullBytes = bb == 0 ? 0 : Math.max(1, bb * opts.getThresholdPercent() / 100);
        pro = PullRequestOptions.builder(repullMessages)
            .maxBytes(repullBytes)
            .expiresIn(opts.getExpires())
            .idleHeartbeat(opts.getIdleHeartbeat())
            .build();

        thresholdMessages = bm - repullMessages;
        thresholdBytes = bb == 0 ? Integer.MIN_VALUE : bb - repullBytes;
    }

    @Override
    public Message nextMessage(long timeoutMillis) throws InterruptedException, IllegalStateException {
        Message msg = sub.nextMessage(timeoutMillis);
        if (msg != null) {
            if (active &&
                (pmm.pendingMessages <= thresholdMessages || pmm.pendingBytes <= thresholdBytes))
            {
                sub.pull(pro);
            }
        }
        return msg;
    }
}
