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
import java.util.concurrent.atomic.AtomicBoolean;

class NatsMessageConsumerBase implements MessageConsumer {
    protected NatsJetStreamPullSubscription sub;
    protected PullMessageManager pmm;
    protected final AtomicBoolean stopped;
    protected final AtomicBoolean finished;
    protected ConsumerInfo cachedConsumerInfo;
    protected String consumerName;

    NatsMessageConsumerBase(ConsumerInfo cachedConsumerInfo) {
        this.cachedConsumerInfo = cachedConsumerInfo;
        if (cachedConsumerInfo != null) {
            this.consumerName = cachedConsumerInfo.getName();
        }
        this.stopped = new AtomicBoolean(false);
        this.finished = new AtomicBoolean(false);
    }

    void setConsumerName(String consumerName) {
        this.consumerName = consumerName;
    }

    void initSub(NatsJetStreamPullSubscription sub) {
        this.sub = sub;
        pmm = (PullMessageManager)sub.manager;
    }

    /**
     * {@inheritDoc}
     */
    public boolean isStopped() {
        return stopped.get();
    }

    /**
     * {@inheritDoc}
     */
    public boolean isFinished() {
        return finished.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getConsumerName() {
        if (consumerName == null) {
            consumerName = cachedConsumerInfo.getName();
        }
        return consumerName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerInfo getConsumerInfo() throws IOException, JetStreamApiException {
        if (cachedConsumerInfo == null) {
            cachedConsumerInfo = sub.getConsumerInfo();
            consumerName = cachedConsumerInfo.getName();
        }
        return cachedConsumerInfo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerInfo getCachedConsumerInfo() {
        return cachedConsumerInfo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        stopped.set(true);
    }

    @Override
    public void close() throws Exception {
        lenientClose();
    }

    protected void finish(boolean close) {
        finished.set(true);
        if (pmm != null) {
            pmm.shutdownHeartbeatTimer();
        }
        if (close) {
            lenientClose();
        }
    }

    protected void lenientClose() {
        try {
            if (!stopped.get() || sub.isActive()) {
                stopped.set(true);
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
