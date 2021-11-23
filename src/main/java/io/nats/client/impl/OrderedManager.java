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
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;

class OrderedManager extends MessageManager {

    private final NatsJetStream js;
    private final NatsDispatcher dispatcher;
    private final String stream;
    private final ConsumerConfiguration serverCC;

    private long lastStreamSeq;
    private long expectedConsumerSeq;

    OrderedManager(NatsJetStream js, NatsDispatcher dispatcher, String stream, ConsumerConfiguration serverCC) {
        this.js = js;
        this.dispatcher = dispatcher;
        this.stream = stream;
        this.serverCC = serverCC;
        lastStreamSeq = -1;
        expectedConsumerSeq = 1; // always starts at 1
    }

    @Override
    boolean manage(Message msg) {
        if (msg != null) {
            long receivedConsumerSeq = msg.metaData().consumerSequence();
            if (expectedConsumerSeq != receivedConsumerSeq) {
                try {
                    expectedConsumerSeq = 1; // new consumer will start at 1

                    // new sub needs a new consumer with a new deliver subject
                    String newDeliver = sub.connection.createInbox();

                    ConsumerConfiguration userCC = ConsumerConfiguration.builder(serverCC)
                        .deliverPolicy(DeliverPolicy.ByStartSequence)
                        .deliverSubject(newDeliver)
                        .startSequence(lastStreamSeq + 1)
                        .startTime(null) // clear start time in case it was originally set
                        .build();

                    js._createConsumer(stream, userCC);
                    sub.reSubscribe(newDeliver);
                }
                catch (Exception e) {
                    IllegalStateException ise = new IllegalStateException("Ordered subscription fatal error.", e);
                    js.conn.processException(ise);
                    if (dispatcher == null) { // synchronous
                        throw ise;
                    }
                }
                return true;
            }
            lastStreamSeq = msg.metaData().streamSequence();
            expectedConsumerSeq = receivedConsumerSeq + 1;
        }
        return false;
    }
}
