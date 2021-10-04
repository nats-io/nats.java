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
import io.nats.client.MessageHandler;
import io.nats.client.SubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;

class NatsJetStreamSubscriptionMessageHandler extends AutoStatusManager implements MessageHandler {
    private final MessageHandler userMH;
    private final boolean autoAck;

    NatsJetStreamSubscriptionMessageHandler(NatsConnection conn, MessageHandler userMH,
                                            boolean autoAck, boolean queueMode,
                                            SubscribeOptions so, ConsumerConfiguration consumerConfig) {
        super(conn, so, consumerConfig, null, queueMode, false);
        this.userMH = userMH;
        this.autoAck = autoAck;
    }

    // This allows for optimization.
    boolean isNecessary() {
        return autoAck || !super.isNoOp();
    }

    @Override
    public void onMessage(Message msg) throws InterruptedException {
        // DISPATCHER CATCHES EXCEPTION SO NOT NECESSARY HERE

        if (preProcess(msg)) {
            return; // pre-processor indicated to terminate processing
        }

        userMH.onMessage(msg);

        // don't ack if not JetStream
        if (autoAck && msg.isJetStream()) {
            msg.ack();
        }
    }
}
