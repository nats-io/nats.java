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
import io.nats.client.MessageConsumer;
import io.nats.client.api.ConsumerInfo;

import java.io.IOException;
import java.time.Duration;

class NatsMessageConsumerBase implements MessageConsumer {
    protected NatsJetStreamPullSubscription sub;
    protected PullMessageManager pmm;
    protected final Object subLock;
    protected boolean stopped;

    NatsMessageConsumerBase() {
        subLock = new Object();
    }

    protected void initSub(NatsJetStreamPullSubscription sub) {
        this.sub = sub;
        pmm = (PullMessageManager)sub.manager;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerInfo getConsumerInfo() throws IOException, JetStreamApiException {
        synchronized (subLock) {
            return sub.getConsumerInfo();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop(long timeout) throws InterruptedException {
        synchronized (subLock) {
            if (!stopped) {
                try {
                    if (sub.getNatsDispatcher() != null) {
                        sub.getDispatcher().drain(Duration.ofMillis(timeout));
                    }
                    else {
                        sub.drain(Duration.ofMillis(timeout));
                    }
                }
                finally {
                    stopped = true;
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (!stopped && sub.isActive()) {
                if (sub.getNatsDispatcher() != null) {
                    sub.getDispatcher().unsubscribe(sub);
                }
                else {
                    sub.unsubscribe();
                }
            }
        }
        catch (Throwable ignore) {
            // nothing to do
        }
    }
}
