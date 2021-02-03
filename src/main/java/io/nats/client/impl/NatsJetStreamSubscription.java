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
 * This is a JetStream specific subscription.
 */
public class NatsJetStreamSubscription extends NatsSubscription implements JetStreamSubscription, NatsJetStreamConstants {

    NatsJetStream js;
    String consumer;
    String stream;
    String deliver;
    int pullBatchSize;

    NatsJetStreamSubscription(String sid, String subject, String queueName, NatsConnection connection,
            NatsDispatcher dispatcher) {
        super(sid, subject, queueName, connection, dispatcher);
    }

    void setupJetStream(NatsJetStream js, String consumer, String stream, String deliver, int pull) {
        this.js = js;
        this.consumer = consumer;
        this.stream = stream;
        this.deliver = deliver;
        this.pullBatchSize = pull;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void poll() {
        if (pullBatchSize == 0) {
            throw new IllegalStateException("Subscription type does not support poll.");
        }

        String subj = js.appendPrefix(String.format(JSAPI_CONSUMER_MSG_NEXT, stream, consumer));
        byte[] payload = String.format("{ \"batch\":%d}", pullBatchSize).getBytes();
        connection.publish(subj, getSubject(), payload);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerInfo getConsumerInfo() throws IOException, JetStreamApiException {
        return js.lookupConsumerInfo(stream, consumer);
    }

    @Override
    public String toString() {
        return "NatsJetStreamSubscription{" +
                "consumer='" + consumer + '\'' +
                ", stream='" + stream + '\'' +
                ", deliver='" + deliver + '\'' +
                ", pull=" + pullBatchSize +
                '}';
    }
}
