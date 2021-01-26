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

import io.nats.client.ConsumerInfo;
import io.nats.client.JetStreamSubscription;

import java.io.IOException;

/**
 * This is a jetstream specfic subscription.
 */
public class NatsJetStreamSubscription extends NatsSubscription implements JetStreamSubscription {

    // JSAPI_REQUEST_NEXT is the prefix for the request next message(s) for a
    // consumer in worker/pull mode.
    private static final String JSAPI_REQUEST_NEXT = "CONSUMER.MSG.NEXT.%s.%s";

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

    /**
     * {@inheritDoc}
     */
    @Override
    public void poll() {
        if (deliver == null || pull == 0) {
            throw new IllegalStateException("Subscription type does not support poll.");
        }

        String subj = js.appendPre(String.format(JSAPI_REQUEST_NEXT, stream, consumer));
        byte[] payload = String.format("{ \"batch\":%d}", pull).getBytes();
        connection.publish(subj, getSubject(), payload);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerInfo getConsumerInfo() throws IOException, JetStreamApiException {
        return js.getConsumerInfo(stream, consumer);
    }

    @Override
    public String toString() {
        return "NatsJetStreamSubscription{" +
                "consumer='" + consumer + '\'' +
                ", stream='" + stream + '\'' +
                ", deliver='" + deliver + '\'' +
                ", pull=" + pull +
                '}';
    }
}
