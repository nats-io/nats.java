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
import io.nats.client.api.ConsumerInfo;
import io.nats.client.support.JsonUtils;
import io.nats.client.support.NatsJetStreamConstants;
import io.nats.client.support.Status;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.support.Validator.validatePullBatchSize;

/**
 * This is a JetStream specific subscription.
 */
public class NatsJetStreamSubscription extends NatsSubscription implements JetStreamSubscription, NatsJetStreamConstants {

    private final NatsJetStream js;
    private final boolean autoProtoManage;
    private final boolean noNextMessage;
    private final boolean pullMode;
    private final AtomicReference<String> lastFcSubject;

    private String consumer;
    private String stream;
    private String deliver;

    NatsJetStreamSubscription(String sid, String subject, String queueName,
                              NatsConnection connection, NatsDispatcher dispatcher,
                              NatsJetStream js, boolean pullMode, boolean autoProtoManage) {
        super(sid, subject, queueName, connection, dispatcher);
        this.js = js;
        this.pullMode = pullMode;
        this.autoProtoManage = autoProtoManage;
        noNextMessage = dispatcher != null;
        lastFcSubject = new AtomicReference<>();
    }

    static class NatsJetStreamSubscriptionMessageHandler implements MessageHandler {
        final NatsConnection conn;
        final MessageHandler userMH;
        final boolean autoAck;
        final boolean autoProtoManage;
        final AtomicReference<String> lastFcSubject;

        // caller must ensure userMH is not null
        NatsJetStreamSubscriptionMessageHandler(NatsConnection conn, MessageHandler userMH, boolean autoAck, boolean autoProtoManage) {
            this.conn = conn;
            this.userMH = userMH;
            this.autoAck = autoAck;
            this.autoProtoManage = autoProtoManage;
            lastFcSubject = new AtomicReference<>();
        }

        @Override
        public void onMessage(Message msg) throws InterruptedException {
            try  {
                if (autoProtoManage && handleIfProtocolMessage(msg, conn, lastFcSubject)) {
                    return;
                }

                if (userMH != null) {
                    userMH.onMessage(msg);
                }

                // don't ack if not JetStream
                if (autoAck && msg.isJetStream()) {
                    msg.ack();
                }
            } catch (Exception e) {
                ErrorListener el = conn.getOptions().getErrorListener();
                if (el != null) {
                    el.exceptionOccurred(conn, e);
                }
            }
        }
    }

    void finishSetup(String stream, String consumer, String deliver) {
        this.consumer = consumer;
        this.stream = stream;
        this.deliver = pullMode ? null : deliver;
    }

    String getConsumer() {
        return consumer;
    }

    String getStream() {
        return stream;
    }

    String getDeliverSubject() {
        return deliver;
    }

    boolean isPullMode() {
        return pullMode;
    }

    boolean isAutoProtoManage() {
        return autoProtoManage;
    }

    @Override
    public Message nextMessage(Duration timeout) throws InterruptedException, IllegalStateException {
        // dispatched means handler means nextMessage should never called
        if (noNextMessage) {
            throw new IllegalStateException("Calling nextMessage not allowed for async push or pull subscriptions.");
        }

        if (autoProtoManage) {
            // null timeout is allowed, it means don't wait, messages must already be available.
            if (timeout == null) {
                Message msg = super.nextMessage(timeout);
                return msg == null || handleIfProtocolMessage(msg, connection, lastFcSubject) ? null : msg;
            }

            long now = System.currentTimeMillis();
            System.out.println("NM-T: " + timeout);
            Message msg = super.nextMessage(timeout);
            while (msg != null && handleIfProtocolMessage(msg, connection, lastFcSubject)) {
                // reduce the timeout by the time spent processing
                long newNow = System.currentTimeMillis();
                timeout = timeout.minusMillis(newNow - now);
                if (timeout.toNanos() > 0) {
                    now = newNow;
                    System.out.println("NM-t: " + timeout);
                    msg = super.nextMessage(timeout);
                }
                else {
                    msg = null;
                }
            }
            return msg;

        }

        // pull, push-sync not auto-proto-managed
        return super.nextMessage(timeout);
    }

    private static boolean handleIfProtocolMessage(Message msg, NatsConnection conn, AtomicReference<String> lastFcSubject) {
        if (msg.isStatusMessage()) {
            Status status = msg.getStatus();
            String fcSubject = null;
            if (status.isFlowControl()) {
                fcSubject = msg.getReplyTo();
            }
            else if (status.isHeartbeat()) {
                Headers h = msg.getHeaders();
                fcSubject = h == null ? null : h.getFirst(CONSUMER_STALLED_HDR);
            }

            // we may get multiple fc/hb messages with the same reply
            // only need to post to that subject once
            if (fcSubject != null && !fcSubject.equals(lastFcSubject.get())) {
                conn.publishInternal(fcSubject, null, null, null, false);
                lastFcSubject.set(fcSubject); // set after publish in case the pub fails
            }
            return true; // handled
        }
        return false; // not handled
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullExpiresIn(int batchSize, long expiresInMillis) {
        _pull(batchSize, false, Duration.ofMillis(expiresInMillis));
    }

    private void _pull(int batchSize, boolean noWait, Duration expiresIn) {
        if (!isPullMode()) {
            throw new IllegalStateException("Subscription type does not support pull.");
        }

        int batch = validatePullBatchSize(batchSize);
        String publishSubject = js.prependPrefix(String.format(JSAPI_CONSUMER_MSG_NEXT, stream, consumer));
        connection.publish(publishSubject, getSubject(), getPullJson(batch, noWait, expiresIn));
        connection.lenientFlushBuffer();
    }

    byte[] getPullJson(int batch, boolean noWait, Duration expiresIn) {
        StringBuilder sb = JsonUtils.beginJson();
        JsonUtils.addField(sb, "batch", batch);
        JsonUtils.addFldWhenTrue(sb, "no_wait", noWait);
        JsonUtils.addFieldAsNanos(sb, "expires", expiresIn);
        return JsonUtils.endJson(sb).toString().getBytes(StandardCharsets.US_ASCII);
    }

    private static final Duration SUBSEQUENT_WAITS = Duration.ofMillis(500);

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Message> fetch(int batchSize, long maxWaitMillis) {
        return fetch(batchSize, Duration.ofMillis(maxWaitMillis));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Message> fetch(int batchSize, Duration maxWait) {
        List<Message> messages = new ArrayList<>(batchSize);

        try {
            pullNoWait(batchSize);
            read(batchSize, maxWait, messages);
            if (messages.size() == 0) {
                pullExpiresIn(batchSize, maxWait.minusMillis(10));
                read(batchSize, maxWait, messages);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return messages;
    }

    private void read(int batchSize, Duration maxWait, List<Message> messages) throws InterruptedException {
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
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Message> iterate(final int batchSize, long maxWaitMillis) {
        return iterate(batchSize, Duration.ofMillis(maxWaitMillis));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Message> iterate(int batchSize, Duration maxWait) {
        pullNoWait(batchSize);

        return new Iterator<Message>() {
            int received = 0;
            boolean finished = false;
            boolean stepDown = true;
            Duration wait = maxWait;
            Message msg = null;

            @Override
            public boolean hasNext() {
                while (!finished && msg == null) {
                    try {
                        msg = nextMessage(wait);
                        wait = SUBSEQUENT_WAITS;
                        if (msg == null) {
                            if (received == 0 && stepDown) {
                                stepDown = false;
                                pullExpiresIn(batchSize, maxWait.minusMillis(10));
                            }
                            else {
                                finished = true;
                            }
                        }
                        else if (msg.isJetStream()) {
                            finished = ++received == batchSize;
                        }
                        else {
                            msg = null;
                        }
                    } catch (InterruptedException e) {
                        msg = null;
                        finished = true;
                        Thread.currentThread().interrupt();
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
                ", isPullMode=" + pullMode +
                '}';
    }
}
