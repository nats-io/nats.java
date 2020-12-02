// Copyright 2020 The NATS Authors
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

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import io.nats.client.ConsumerInfo;
import io.nats.client.JetStreamSubscription;

/**
 * This is a jetstream specfic subscription.
 */
public class NatsJetStreamSubscription extends NatsSubscription implements JetStreamSubscription {

    // JSApiRequestNextT is the prefix for the request next message(s) for a
    // consumer in worker/pull mode.
    private static final String jSApiRequestNextT = "CONSUMER.MSG.NEXT.%s.%s";

    NatsJetStream js;
    String consumer;
    String stream;
    String deliver;
    long pull;

    NatsJetStreamSubscription(String sid, String subject, String queueName, NatsConnection connection,
            NatsDispatcher dispatcher) {
        super(sid, subject, queueName, connection, dispatcher);
    }

    void setupJetStream(NatsJetStream js, String consumer, String stream, String deliver, long pull) {
        this.js = js;
        this.consumer = consumer;
        this.stream = stream;
        this.deliver = deliver;
        this.pull = pull;
    }

    @Override
    public void poll() {
        if (deliver == null || pull == 0) {
            throw new IllegalStateException("Subsription type does not support poll.");
        }

        String subj = js.appendPre(String.format(jSApiRequestNextT, stream, consumer));
        byte[] payload = String.format("{ \"batch\":%d}", pull).getBytes();
        connection.publish(subj, getSubject(), payload);
    }

    @Override
    public ConsumerInfo getConsumerInfo() throws IOException, TimeoutException, InterruptedException {
        return js.getConsumerInfo(stream, consumer);
    }
    
}
