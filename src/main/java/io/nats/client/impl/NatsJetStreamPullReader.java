// Copyright 2022 The NATS Authors
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

import io.nats.client.JetStreamReader;
import io.nats.client.Message;

import java.time.Duration;

class NatsJetStreamPullReader extends NatsSimpleConsumer implements JetStreamReader {
    private final int batchSize;
    private final int repullAt;
    private final Object keepGoingLock;
    private boolean keepGoing;
    private int currentBatchRed;

    public NatsJetStreamPullReader(final NatsJetStreamPullSubscription sub, final int batchSize, final int repullAt) {
        super(sub);
        this.batchSize = batchSize;
        this.repullAt = Math.max(1, Math.min(batchSize, repullAt));
        keepGoingLock = new Object();
        currentBatchRed = 0;
        keepGoing = true;
        sub.pull(batchSize);
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
                synchronized (keepGoingLock) {
                    if (keepGoing) {
                        sub.pull(batchSize);
                    }
                }
            }
            if (currentBatchRed == batchSize) {
                currentBatchRed = 0;
            }
        }
        return msg;
    }

    @Override
    public void unsubscribe(int after) {
        synchronized (keepGoingLock) {
            keepGoing = false;
        }
        super.unsubscribe(after);
    }
}
