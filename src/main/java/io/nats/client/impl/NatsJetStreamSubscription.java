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

import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.support.JsonUtils;
import io.nats.client.support.NatsJetStreamConstants;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static io.nats.client.support.Validator.validatePullBatchSize;

/**
 * This is a JetStream specific subscription.
 */
public class NatsJetStreamSubscription extends NatsSubscription implements JetStreamSubscription, NatsJetStreamConstants {

    private final NatsJetStream js;
    private final boolean pullMode;
    private final boolean asyncMode;

    private final String stream;
    private final String consumerName;
    private final String deliver;

    private final NatsJetStreamAutoStatusManager asm;

    NatsJetStreamSubscription(String sid, String subject, String queueName,
                              NatsConnection connection, NatsDispatcher dispatcher,
                              NatsJetStreamAutoStatusManager asm,
                              NatsJetStream js, boolean pullMode,
                              String stream, String consumer, String deliver) {
        super(sid, subject, queueName, connection, dispatcher);
        this.asm = asm; // might be null, that's okay it's async
        this.js = js;
        this.pullMode = pullMode;
        this.asyncMode = dispatcher != null;
        this.stream = stream;
        this.consumerName = consumer;
        this.deliver = pullMode ? null : deliver;

        // 'sync push' and 'pull' are not dispatched. nextMessage will use the manager (asm)
        // 'async push' will have a version of the manager in the handler we wrap the user's handler in,
        // since nextMessage won't be called. That handler is setup before this object is created.
    }

    String getConsumerName() {
        return consumerName;
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

    @Override
    void invalidate() {
        asm.shutdown();
        super.invalidate();
    }

    @Override
    public Message nextMessage(Duration timeout) throws InterruptedException, IllegalStateException {

        if (asyncMode) { // asm will be null for push async, who should not be calling this method
            return null; // maybe next version we make this throw an exception
        }

        // timeout null means don't wait at all, timeout <= 0 means wait forever
        // until we get an actual no (null) message or we get a message
        // that the manager (asm) does not handle (asm.preProcess would be false)
        if (timeout == null || timeout.toMillis() <= 0) {
            Message msg = super.nextMessage(timeout);
            while (msg != null && asm.manage(msg)) {
                msg = super.nextMessage(timeout);
            }
            return msg;
        }

        return nextMessageGtZeroTimeout(timeout);
    }

    private Message nextMessageGtZeroTimeout(Duration timeout) throws InterruptedException {
        // timeout >= 0 process as many messages we can in that time period
        // if we get a message that the asm handles, we try again, but
        // with a shorter timeout based on what we already used up
        long endTime = System.currentTimeMillis() + timeout.toMillis();
        Message msg = super.nextMessage(timeout);
        while (msg != null && asm.manage(msg)) {
            msg = null;
            long millis = endTime - System.currentTimeMillis();
            if (millis > 0) {
                msg = super.nextMessage(Duration.ofMillis(millis));
            }
        }
        return msg;
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
        durationGtZeroRequired(expiresIn, "Expires In");
        _pull(batchSize, false, expiresIn);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullExpiresIn(int batchSize, long expiresInMillis) {
        pullExpiresIn(batchSize, Duration.ofMillis(expiresInMillis));
    }

    private void _pull(int batchSize, boolean noWait, Duration expiresIn) {
        if (!isPullMode()) {
            throw new IllegalStateException("Subscription type does not support pull.");
        }

        int batch = validatePullBatchSize(batchSize);
        String publishSubject = js.prependPrefix(String.format(JSAPI_CONSUMER_MSG_NEXT, stream, consumerName));
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
        durationGtZeroRequired(maxWait, "Fetch max");

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

    private void durationGtZeroRequired(Duration duration, String label) {
        if (duration == null || duration.toMillis() <= 0) {
            throw new IllegalArgumentException(label + " must be supplied and greater than 0.");
        }
    }

    private void read(int batchSize, Duration timeout, List<Message> messages) throws InterruptedException {
        Message msg = nextMessageGtZeroTimeout(timeout);
        while (msg != null) {
            messages.add(msg);
            msg = null;
            if (messages.size() < batchSize) {
                msg = nextMessageGtZeroTimeout(timeout);
            }
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
            final long timeout = maxWait.toMillis();
            int received = 0;
            boolean finished = false;
            boolean stepDown = true;
            Message msg = null;

            @Override
            public boolean hasNext() {
                long end = System.currentTimeMillis() + timeout;
                while (!finished && msg == null) {
                    try {
                        msg = nextMessage(timeout);
                        long wait = end - System.currentTimeMillis();
                        if (msg == null) {
                            if (received == 0 && stepDown) {
                                stepDown = false;
                                pullExpiresIn(batchSize, Duration.ofMillis(wait - 10));
                            }
                            else {
                                finished = true;
                            }
                        }
                        else {
                            finished = ++received == batchSize;
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
        return js.lookupConsumerInfo(stream, consumerName);
    }

    @Override
    public String toString() {
        return "NatsJetStreamSubscription{" +
                "consumer='" + consumerName + '\'' +
                ", stream='" + stream + '\'' +
                ", deliver='" + deliver + '\'' +
                ", isPullMode=" + pullMode +
                '}';
    }
}
