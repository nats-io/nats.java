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

package io.nats.examples.testapp_mc;

import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class AdvancedSubscription implements MessageHandler {
    static int ASID_GEN = 64;  // intentionally static so crosses all ASubs
    private int SUBID_GEN = 0; // instance scoped

    public final String asid;
    public final String stream;
    public final String subject;
    public final AtomicLong lastAckedSequence;
    public final AtomicReference<CompletableFuture<Boolean>> drainFuture;
    
    public PushSubscribeOptions pso;
    public JetStreamSubscription sub;
    public int subid;

    public AdvancedSubscription(String stream, String subject) {
        this.stream = stream;
        this.subject = subject;
        asid = "ASUB-" + (char)++ASID_GEN;
        this.lastAckedSequence = new AtomicLong(0);
        drainFuture = new AtomicReference<>();
    }

    @Override
    public void onMessage(Message msg) throws InterruptedException {
        PushApp.println(asid + "-" + subid, "Message " + msg.getSubject() + " / " + new String(msg.getData()));
        msg.ack();
        lastAckedSequence.set(msg.metaData().streamSequence());
    }

    public void setSub(JetStreamSubscription newsub) {
        sub = newsub;
        subid = ++SUBID_GEN;
    }

    public PushSubscribeOptions getPso() {
        ConsumerConfiguration.Builder ccb = ConsumerConfiguration.builder()
            .filterSubject(subject)
            .flowControl(PushApp.IDLE_HEARTBEAT)
            .inactiveThreshold(PushApp.INACTIVE_THRESHOLD);

        if (lastAckedSequence.get() > 0) {
            ccb.deliverPolicy(DeliverPolicy.ByStartSequence)
                .startSequence(lastAckedSequence.get() + 1);
        }

        pso = PushSubscribeOptions.builder()
            .stream(stream)
            .configuration(ccb.build())
            .build();

        return pso;
    }
}
