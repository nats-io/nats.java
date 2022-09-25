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
import io.nats.client.PullRequestOptions;
import io.nats.client.SimpleConsumerOptions;

import java.time.Duration;

class NatsJetStreamPullReader extends NatsSimpleConsumer implements JetStreamReader {
    private final Object keepGoingLock;
    private boolean keepGoing;
    private int currentBatchRed;

    public NatsJetStreamPullReader(final NatsJetStreamPullSubscription sub, SimpleConsumerOptions sco) {
        super(sub, sco);
        keepGoingLock = new Object();
        currentBatchRed = 0;
        keepGoing = true;
        sub.pull(PullRequestOptions.builder(sco.batchSize)
            .maxBytes(sco.maxBytes)
            .expiresIn(sco.expiresIn)
            .idleHeartbeat(sco.idleHeartbeat)
            .build());
    }

    @Override
    public Message nextMessage(Duration timeout) throws InterruptedException, IllegalStateException {
        return sub.nextMessage(timeout);
//        return track(sub.nextMessage(timeout));
    }

    @Override
    public Message nextMessage(long timeoutMillis) throws InterruptedException, IllegalStateException {
        return sub.nextMessage(timeoutMillis);
//        return track(sub.nextMessage(timeoutMillis));
    }

    private Message track(Message msg) {
        if (msg != null) {
            if (++currentBatchRed == sco.repullAt) {
                synchronized (keepGoingLock) {
                    if (keepGoing) {
                        sub.pull(PullRequestOptions.builder(sco.batchSize)
                            .maxBytes(sco.maxBytes)
                            .expiresIn(sco.expiresIn)
                            .idleHeartbeat(sco.idleHeartbeat)
                            .build());
                    }
                }
            }
            if (currentBatchRed == sco.batchSize) {
                currentBatchRed = 0;
            }
        }
        return msg;
    }

//    @Override
//    public void unsubscribe(int after) {
//        synchronized (keepGoingLock) {
//            keepGoing = false;
//        }
//        super.unsubscribe(after);
//    }
}

