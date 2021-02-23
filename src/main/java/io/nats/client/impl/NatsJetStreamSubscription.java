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
import io.nats.client.PullSubscribeOptions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static io.nats.client.support.Validator.validatePullBatchSize;

/**
 * This is a JetStream specific subscription.
 */
public class NatsJetStreamSubscription extends NatsSubscription implements JetStreamSubscription, NatsJetStreamConstants {

    private NatsJetStream js;
    private String consumer;
    private String stream;
    private String deliver;
    private PullSubscribeOptions pullSubscribeOptions;

    NatsJetStreamSubscription(String sid, String subject, String queueName, NatsConnection connection,
            NatsDispatcher dispatcher) {
        super(sid, subject, queueName, connection, dispatcher);
    }

    void setupJetStream(NatsJetStream js, String consumer, String stream, String deliver, PullSubscribeOptions pullSubscribeOptions) {
        this.js = js;
        this.consumer = consumer;
        this.stream = stream;
        this.deliver = deliver;
        this.pullSubscribeOptions = pullSubscribeOptions;
    }

    boolean isPullMode() {
        return pullSubscribeOptions != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pull(int batchSize) {
        _pull(batchSize, false, null); // nulls mean use default
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullNoWait(int batchSize) {
        _pull(batchSize, true, null); // nulls mean use default
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullExpiresIn(int batchSize, Duration expiresIn) {
        _pull(batchSize, false, expiresIn); // nulls mean use default
    }

    private void _pull(int batchSize, boolean noWait, Duration expiresIn) {
        if (!isPullMode()) {
            throw new IllegalStateException("Subscription type does not support pull.");
        }

        int batch = validatePullBatchSize(batchSize);
        String publishSubject = js.appendPrefix(String.format(JSAPI_CONSUMER_MSG_NEXT, stream, consumer));
        connection.publish(publishSubject, getSubject(), getPullJson(noWait, expiresIn, batch));
        connection.lenientFlushBuffer();
    }

    byte[] getPullJson(boolean noWait, Duration expiresIn, int batch) {
        StringBuilder sb = JsonUtils.beginJson();
        JsonUtils.addFld(sb, "batch", batch);
        JsonUtils.addFldWhenTrue(sb, "no_wait", noWait);
        if (expiresIn != null) {
            JsonUtils.addFld(sb, "expires", DateTimeUtils.fromNow(expiresIn));
        }
        return JsonUtils.endJson(sb).toString().getBytes(StandardCharsets.US_ASCII);
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
                ", isPullMode='" + isPullMode() +
                '}';
    }
}
