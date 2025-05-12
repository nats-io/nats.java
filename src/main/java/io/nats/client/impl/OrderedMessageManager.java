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

import io.nats.client.JetStreamManagement;
import io.nats.client.Message;
import io.nats.client.SubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerCreateRequest;
import io.nats.client.api.ConsumerInfo;

import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.impl.MessageManager.ManageResult.MESSAGE;
import static io.nats.client.impl.MessageManager.ManageResult.STATUS_HANDLED;

class OrderedMessageManager extends PushMessageManager {

    protected long expectedExternalConsumerSeq;
    protected final AtomicReference<String> targetSid;

    protected OrderedMessageManager(
        NatsConnection conn,
        NatsJetStream js,
        String stream,
        SubscribeOptions so,
        ConsumerConfiguration originalCc,
        boolean queueMode,
        boolean syncMode)
    {
        super(conn, js, stream, so, originalCc, queueMode, syncMode);
        expectedExternalConsumerSeq = 1; // always starts at 1
        targetSid = new AtomicReference<>();
    }

    @Override
    protected void startup(NatsJetStreamSubscription sub) {
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
            if (expectedExternalConsumerSeq != receivedConsumerSeq) {
                handleErrorCondition();
                return STATUS_HANDLED;
            }
            trackJsMessage(msg);
            expectedExternalConsumerSeq++;
            return MESSAGE;
        }

        return manageStatus(msg);
    }

    @Override
    protected void handleHeartbeatError() {
        super.handleHeartbeatError();
        handleErrorCondition();
    }

    private void handleErrorCondition() {
        try {
            targetSid.set(null);
            expectedExternalConsumerSeq = 1; // consumer always starts with consumer sequence 1

            // 1. delete the consumer by name so we can recreate it with a different delivery policy
            //    b/c we cannot edit a push consumer's delivery policy
            JetStreamManagement jsm = conn.jetStreamManagement(js.jso);
            String actualConsumerName = sub.getConsumerName();
            try {
                jsm.deleteConsumer(stream, actualConsumerName);
            }
            catch (Exception ignore) {}

            // 2. re-subscribe. This means killing the sub then making a new one.
            //    New sub needs a new deliverSubject
            String newDeliverSubject = sub.connection.createInbox();
            sub.reSubscribe(newDeliverSubject);
            targetSid.set(sub.getSID());

            // 3. make a new consumer using the same "deliver" subject
            //    but with a new starting point
            ConsumerConfiguration userCC = js.consumerConfigurationForOrdered(originalCc, lastStreamSeq, newDeliverSubject, actualConsumerName, null);
            ConsumerInfo ci = js._createConsumer(stream, userCC, ConsumerCreateRequest.Action.Create); // this can fail when a server is down.
            sub.setConsumerName(ci.getName());

            // 4. restart the manager.
            startup(sub);
        }
        catch (Exception e) {
            // don't want this doubly failing for any reason
            try {
                js.conn.processException(e);
            }
            catch (Exception ignore) {}
            initOrResetHeartbeatTimer();
        }
    }
}
