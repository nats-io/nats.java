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

import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.SubscribeOptions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static io.nats.client.impl.Validator.validatePullBatchSize;

/**
 * This is a JetStream specific subscription.
 */
public class NatsJetStreamSubscription extends NatsSubscription implements JetStreamSubscription, NatsJetStreamConstants {

    private NatsJetStream js;
    private String consumer;
    private String stream;
    private String deliver;
    private boolean isPullMode;

    NatsJetStreamSubscription(String sid, String subject, String queueName, NatsConnection connection,
            NatsDispatcher dispatcher) {
        super(sid, subject, queueName, connection, dispatcher);
    }

    void setupJetStream(NatsJetStream js, String consumer, String stream, String deliver, SubscribeOptions subscribeOptions) {
        this.js = js;
        this.consumer = consumer;
        this.stream = stream;
        this.deliver = deliver;
        isPullMode = subscribeOptions instanceof PullSubscribeOptions;
    }

    boolean isPullMode() {
        return isPullMode;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pull(int batchSize) {
        _pull(batchSize, false, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullNoWait(int batchSize) {
        _pull(batchSize, true, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullExpiresIn(int batchSize, Duration expiresIn) {
        _pull(batchSize, false, expiresIn);
    }

    private void _pull(int batchSize, boolean noWait, Duration expiresIn) {
        if (!isPullMode()) {
            throw new IllegalStateException("Subscription type does not support pull.");
        }

        int batch = validatePullBatchSize(batchSize);
        String publishSubject = js.prependPrefix(String.format(JSAPI_CONSUMER_MSG_NEXT, stream, consumer));
        connection.publish(publishSubject, getSubject(), getPullJson(batch, noWait, expiresIn, null));
        connection.lenientFlushBuffer();
    }

    byte[] getPullJson(int batch, boolean noWait, Duration expiresIn, String prefix) {
        StringBuilder sb = JsonUtils.beginJsonPrefixed(prefix);
        JsonUtils.addFld(sb, "batch", batch);
        JsonUtils.addFldWhenTrue(sb, "no_wait", noWait);
        JsonUtils.addNanoFld(sb, "expires", expiresIn);
        return JsonUtils.endJson(sb).toString().getBytes(StandardCharsets.US_ASCII);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerInfo getConsumerInfo() throws IOException, JetStreamApiException {
        return js.lookupConsumerInfo(stream, consumer);
    }

    private interface InternalBatchHandler {
        boolean onMessage(Message message) throws InterruptedException;
    }

    private static final Duration SUBSEQUENT_WAITS = Duration.ofMillis(500);

    @Override
    public List<Message> fetch(int batchSize, Duration maxWait) {
        List<Message> messages = new ArrayList<>(batchSize);

        try {
            pullExpiresIn(batchSize, maxWait.minusMillis(10));
            Message msg = nextMessage(maxWait);
            while (msg != null) {
                if (msg.isJetStream()) {
                    messages.add(msg);
                    if (messages.size() == batchSize) {
                        break;
                    }
                }
                msg = nextMessage(SUBSEQUENT_WAITS);
            }
        } catch (InterruptedException e) {
            // ignore
        }

        return messages;
    }

    @Override
    public Iterator<Message> iterate(final int batchSize, Duration maxWait) {
        pullExpiresIn(batchSize, maxWait.minusMillis(10));

        return new Iterator<Message>() {
            int received = 0;
            boolean finished = false;
            Duration wait = maxWait;
            Message msg;

            @Override
            public boolean hasNext() {
                while (!finished && msg == null) {
                    try {
                        msg = nextMessage(wait);
                        wait = SUBSEQUENT_WAITS;
                        if (msg == null) {
                            finished = true;
                            break;
                        }
                        if (msg.isJetStream()) {
                            finished = ++received == batchSize;
                            break;
                        }
                        else {
                            msg = null;
                        }
                    } catch (InterruptedException e) {
                        msg = null;
                        finished = true;
                    }
                }
                return msg != null;
            }

            @Override
            public Message next() {
                Message next = msg;
                msg = null;
                return next;
            }
        };
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
