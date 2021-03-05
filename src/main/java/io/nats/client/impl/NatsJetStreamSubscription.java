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

import io.nats.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static io.nats.client.support.Validator.validatePullBatchSize;

/**
 * This is a JetStream specific subscription.
 */
public class NatsJetStreamSubscription extends NatsSubscription implements JetStreamSubscription, NatsJetStreamConstants {

    private NatsJetStream js;
    private String consumer;
    private String stream;
    private String deliver;
    private SubscribeOptions subscribeOptions;
    private boolean isPullMode;
    private boolean isPullSmart;

    NatsJetStreamSubscription(String sid, String subject, String queueName, NatsConnection connection,
            NatsDispatcher dispatcher) {
        super(sid, subject, queueName, connection, dispatcher);
    }

    void setupJetStream(NatsJetStream js, String consumer, String stream, String deliver, SubscribeOptions subscribeOptions) {
        this.js = js;
        this.consumer = consumer;
        this.stream = stream;
        this.deliver = deliver;
        this.subscribeOptions = subscribeOptions;
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

    private boolean lastNotWait = false;
    private Duration lastExpiresIn = null;

    private void _pull(int batchSize, boolean noWait, Duration expiresIn) {
        if (!isPullMode()) {
            throw new IllegalStateException("Subscription type does not support pull.");
        }

        int batch = validatePullBatchSize(batchSize);
        lastNotWait = noWait;
        lastExpiresIn = expiresIn;
        String publishSubject = js.prependPrefix(String.format(JSAPI_CONSUMER_MSG_NEXT, stream, consumer));
        connection.publish(publishSubject, getSubject(), getPullJson(batch, noWait, expiresIn, null));
        connection.lenientFlushBuffer();
    }

// SFF possible behavior
//    byte[] getPrefixedPullJson(String prefix) {
//        return getPullJson(1, lastNotWait, lastExpiresIn, prefix);
//    }

    byte[] getPullJson(int batch, boolean noWait, Duration expiresIn, String prefix) {
        StringBuilder sb = JsonUtils.beginJsonPrefixed(prefix);
        JsonUtils.addFld(sb, "batch", batch);
        JsonUtils.addFldWhenTrue(sb, "no_wait", noWait);
        JsonUtils.addNanoFld(sb, "expires", expiresIn);
        return JsonUtils.endJson(sb).toString().getBytes(StandardCharsets.US_ASCII);
    }

// SFF possible behavior
//    @Override
//    public Message nextMessage(Duration timeout) throws InterruptedException, IllegalStateException {
//        Message msg = super.nextMessage(timeout);
//        return msg == null && isPullSmart ? get404Message() : msg;
//    }
//
//    Message message404; // lazy init
//    private Message get404Message() {
//        if (message404 == null) {
//            message404 = new NatsMessage.StatusMessage(404, "No Messages");
//        }
//        return message404;
//    }

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

    private void batchInternal(int batchSize, Duration timeout, InternalBatchHandler handler) {
        boolean keepGoing;
        try {
            pullNoWait(batchSize);
            keepGoing = handler.onMessage(nextMessage(timeout));
        } catch (InterruptedException e) {
            keepGoing = false;
        }

        while (keepGoing) {
            try {
                keepGoing = handler.onMessage(nextMessage(js.getRequestTimeout()));
            } catch (InterruptedException e) {
                keepGoing = false;
            }
        }
    }

    @Override
    public List<Message> fetch(int batchSize, Duration timeout) {
        List<Message> messages = new ArrayList<>(batchSize);

        batchInternal(batchSize, timeout, msg -> {
            if (msg != null && msg.isJetStream()) {
                messages.add(msg);
            }
            return msg != null && messages.size() < batchSize;
        });

        return messages;
    }

    @Override
    public void receive(int batchSize, Duration timeout, MessageHandler handler) {
        connection.executorSubmit(() ->
            batchInternal(batchSize, timeout, msg -> {
                if (msg == null || msg.isJetStream()) {
                    handler.onMessage(msg);
                }
                return msg != null;
            }));
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
