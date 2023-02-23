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

import io.nats.client.ConsumeOptions;
import io.nats.client.Message;
import io.nats.client.MessageNextConsumer;
import io.nats.client.PullRequestOptions;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class NatsMessageNextConsumer extends NatsMessageConsumer implements MessageNextConsumer {
    private final PullRequestOptions pro;
    private final int repullAt;
    private int currentBatchRed;
    private boolean keepGoing = true;

    public NatsMessageNextConsumer(NatsJetStreamPullSubscription sub, ConsumeOptions options) {
        super(sub, options);
        pro = PullRequestOptions.builder(options.getBatchSize()).expiresIn(options.getExpiresIn()).build();
        repullAt = options.getRepullAt();
        currentBatchRed = 0;
        sub.pull(pro);
    }

    @Override
    public Message nextMessage(Duration timeout) throws InterruptedException, IllegalStateException {
        return track(sub.nextMessage(timeout));
    }

    @Override
    public Message nextMessage(long timeoutMillis) throws InterruptedException, IllegalStateException {
        return track(sub.nextMessage(timeoutMillis));
    }

    private Message track(Message msg) {
        if (msg != null) {
            if (++currentBatchRed == repullAt) {
                if (keepGoing) {
                    sub.pull(pro);
                }
            }
            if (currentBatchRed == pro.getBatchSize()) {
                currentBatchRed = 0;
            }
        }
        return msg;
    }

    @Override
    public void unsubscribe() {
        keepGoing = false;
        super.unsubscribe();
    }

    @Override
    public void unsubscribe(int after) {
        keepGoing = false;
        super.unsubscribe(after);
    }

    @Override
    public CompletableFuture<Boolean> drain(Duration timeout) throws InterruptedException {
        keepGoing = false;
        return super.drain(timeout);
    }
}
