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

import io.nats.client.JetStreamApiException;
import io.nats.client.SimpleConsumer;
import io.nats.client.api.ConsumerInfo;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

class NatsSimpleConsumerBase implements SimpleConsumer {
    protected NatsJetStreamPullSubscription sub;
    protected PullMessageManager pmm;
    protected final Object subLock;
    protected boolean active;

    NatsSimpleConsumerBase() {
        subLock = new Object();
        active = true;
    }

    // Synchronized by caller if necessary
    protected void initSub(NatsJetStreamPullSubscription sub) {
        this.sub = sub;
        pmm = (PullMessageManager)sub.manager;
    }

    @Override
    public ConsumerInfo getConsumerInfo() throws IOException, JetStreamApiException {
        synchronized (subLock) {
            return sub.getConsumerInfo();
        }
    }

    @Override
    public boolean isActive() {
        return active;
    }

    @Override
    public void unsubscribe() {
        unsubscribe(-1);
    }

    @Override
    public void unsubscribe(int after) {
        synchronized (subLock) {
            active = false;
            if (sub.getNatsDispatcher() != null) {
                sub.getDispatcher().unsubscribe(sub, after);
            }
            else {
                sub.unsubscribe(after);
            }
        }
    }

    @Override
    public CompletableFuture<Boolean> drain(Duration timeout) throws InterruptedException {
        synchronized (subLock) {
            active = false;
            if (sub.getNatsDispatcher() != null) {
                return sub.getDispatcher().drain(timeout);
            }
            return sub.drain(timeout);
        }
    }
}
