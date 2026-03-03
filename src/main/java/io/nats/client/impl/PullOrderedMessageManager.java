// Copyright 2023 The NATS Authors
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
import io.nats.client.SubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.impl.MessageManager.ManageResult.MESSAGE;
import static io.nats.client.impl.MessageManager.ManageResult.STATUS_HANDLED;

class PullOrderedMessageManager extends PullMessageManager {

    protected final ConsumerConfiguration originalCc;
    protected final NatsJetStream js;
    protected final String stream;
    protected final AtomicLong expectedExternalConsumerSeq;
    protected final AtomicReference<String> targetSid;

    protected PullOrderedMessageManager(NatsConnection conn,
                                        NatsJetStream js,
                                        String stream,
                                        SubscribeOptions so, ConsumerConfiguration originalCc, boolean syncMode) {
        super(conn, so, syncMode);
        this.js = js;
        this.stream = stream;
        this.originalCc = originalCc;
        expectedExternalConsumerSeq = new AtomicLong(1); // always starts at 1
        targetSid = new AtomicReference<>();
    }

    @Override
    protected void startup(NatsJetStreamSubscription sub) {
        expectedExternalConsumerSeq.set(1); // consumer always starts with consumer sequence 1
        super.startup(sub);
        targetSid.set(sub.getSID());
    }

    @Override
    protected ManageResult manage(Message msg) {
        if (!msg.getSID().equals(targetSid.get())) {
            return STATUS_HANDLED; // wrong sid. message is a throwaway from previous consumer that errored
        }

        if (msg.isJetStream()) {
            long receivedConsumerSeq = msg.metaData().consumerSequence();
            if (expectedExternalConsumerSeq.get() != receivedConsumerSeq) {
                targetSid.set(null);
                expectedExternalConsumerSeq.set(1); // consumer always starts with consumer sequence 1
                if (pullManagerObserver != null) {
                    pullManagerObserver.pullTerminatedByError();
                }
                return STATUS_HANDLED;
            }
            trackJsMessage(msg);
            checkForPin(msg);
            expectedExternalConsumerSeq.incrementAndGet();
            return MESSAGE;
        }

        return manageStatus(msg);
    }
}
