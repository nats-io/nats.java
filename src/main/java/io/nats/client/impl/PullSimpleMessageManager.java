// Copyright 2021 The NATS Authors
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

import io.nats.client.Message;
import io.nats.client.PullRequestOptions;
import io.nats.client.SimpleConsumerOptions;

class PullSimpleMessageManager extends PullMessageManager {

    private final SimpleConsumerOptions sco;
    private int currentBatchRedMessages;
    private int currentBatchRedBytes;
    private final Object keepGoingLock;
    private boolean keepGoing;


    public PullSimpleMessageManager(NatsConnection conn, boolean syncMode, SimpleConsumerOptions sco) {
        super(conn, syncMode);
        this.sco = sco;
        keepGoingLock = new Object();
        currentBatchRedMessages = 0;
        keepGoing = true;
    }

    @Override
    void startup(NatsJetStreamSubscription sub) {
        super.startup(sub);
        if (!syncMode) {
            sub.pull(PullRequestOptions.builder(sco.batchSize)
                .maxBytes(sco.maxBytes)
                .expiresIn(sco.expiresIn)
                .idleHeartbeat(sco.idleHeartbeat)
                .build());
        }
    }

    @Override
    void shutdown() {
        synchronized (keepGoingLock) {
            keepGoing = false;
        }
        super.shutdown();
    }

    @Override
    protected void subManage(Message msg) {
        System.out.println("SUB MANAGE");
        if (++currentBatchRedMessages == sco.repullAt) {
            synchronized (keepGoingLock) {
                if (keepGoing) {
                    System.out.println("RE PULL");
                    sub.pull(PullRequestOptions.builder(sco.batchSize)
                        .maxBytes(sco.maxBytes)
                        .expiresIn(sco.expiresIn)
                        .idleHeartbeat(sco.idleHeartbeat)
                        .build());
                }
            }
        }
        if (currentBatchRedMessages == sco.batchSize) {
            currentBatchRedMessages = 0;
        }
    }
}
