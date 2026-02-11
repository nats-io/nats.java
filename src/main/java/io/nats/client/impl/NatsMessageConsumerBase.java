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
import io.nats.client.PullRequestOptions;
import io.nats.client.api.ConsumerInfo;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

abstract class NatsMessageConsumerBase implements MessageConsumer, PullManagerObserver {
    protected NatsJetStreamPullSubscription sub;
    protected PullMessageManager pmm;
    protected final AtomicBoolean stopped;
    protected final AtomicBoolean finished;
    protected ConsumerInfo cachedConsumerInfo;
    protected String consumerName;

    static AtomicInteger ID = new AtomicInteger(64);
    protected final String id;

    NatsMessageConsumerBase(ConsumerInfo cachedConsumerInfo) {
        id = "" + (char)ID.incrementAndGet();
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

    void initSub(NatsJetStreamPullSubscription sub, boolean clearCachedConsumerInfo) {
        this.sub = sub;
        this.consumerName = sub.getConsumerName();
        if (clearCachedConsumerInfo) {
            cachedConsumerInfo = null;
        }
        pmm = (PullMessageManager)sub.manager;
    }

    protected void rePull() {
        // may or may not be implemented
        // fetch does not implement
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
        if (consumerName == null && cachedConsumerInfo != null) {
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
        stopped.set(true);
        shutdownSub();
    }

    protected void fullClose() {
        stopped.set(true);
        finished.set(true);
        shutdownSub();
    }

    protected void shutdownSub() {
        try {
            if (sub.isActive()) {
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
        if (pmm != null) {
            try {
                pmm.shutdownHeartbeatTimer();
            }
            catch (Throwable ignore) {
                // nothing to do
            }
        }
    }

    static class PinnablePullRequestOptions extends PullRequestOptions {
        final String pinId;

        public PinnablePullRequestOptions(String pinId, Builder b) {
            super(b);
            this.pinId = pinId;
        }

        @Override
        protected String getPinId() {
            return pinId;
        }
    }
}
