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
import io.nats.client.JetStreamApiException;
import io.nats.client.MessageConsumer;
import io.nats.client.api.ConsumerInfo;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class NatsMessageConsumer implements MessageConsumer {
    protected final NatsJetStreamPullSubscription sub;
    protected final ConsumeOptions options;

    public NatsMessageConsumer(NatsJetStreamPullSubscription sub, ConsumeOptions options) {
        this.sub = sub;
        this.options = options;
    }

    @Override
    public ConsumerInfo getConsumerInfo() throws IOException, JetStreamApiException {
        return sub.getConsumerInfo();
    }

    @Override
    public void unsubscribe() {
        unsubscribe(-1);
    }

    @Override
    public void unsubscribe(int after) {
        if (sub.getNatsDispatcher() != null) {
            sub.getDispatcher().unsubscribe(sub, after);
            sub.getNatsDispatcher().stop(false);
        }
        else {
            sub.unsubscribe(after);
        }
    }

    @Override
    public CompletableFuture<Boolean> drain(Duration timeout) throws InterruptedException {
        if (sub.getNatsDispatcher() != null) {
            return sub.getDispatcher().drain(timeout);
        }
        return sub.drain(timeout);
    }
}
