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
import io.nats.client.support.JsonUtils;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static io.nats.client.support.Validator.validatePullBatchSize;

public class NatsJetStreamPullSubscription extends NatsJetStreamSubscription {

    NatsJetStreamPullSubscription(String sid, String subject,
                                  NatsConnection connection,
                                  NatsJetStream js,
                                  String stream, String consumer,
                                  MessageManager statusManager) {
        super(sid, subject, null, connection, null, js, stream, consumer, statusManager);
    }

    /*
        NatsJetStreamSubscription(String sid, String subject, String queueName,
                              NatsConnection connection, NatsDispatcher dispatcher,
                              NatsJetStream js,
                              String stream, String consumer,
                              MessageManager... managers) {

     */
    @Override
    boolean isPullMode() {
        return true;
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
            long endTime = System.currentTimeMillis() + maxWait.toMillis();
            read(messages, batchSize, endTime);
            if (messages.size() == 0) {
                long expiresIn = endTime - System.currentTimeMillis() - 10;
                if (expiresIn > 0) {
                    pullExpiresIn(batchSize, expiresIn);
                    read(messages, batchSize, endTime);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return messages;
    }

    private void read(List<Message> messages, int batchSize, long endTime) throws InterruptedException {
        Message msg = nextMessageWithEndTime(endTime);
        while (msg != null) {
            messages.add(msg);
            msg = null;
            if (messages.size() < batchSize) {
                msg = nextMessageWithEndTime(endTime);
            }
        }
    }

    private void durationGtZeroRequired(Duration duration, String label) {
        if (duration == null || duration.toMillis() <= 0) {
            throw new IllegalArgumentException(label + " must be supplied and greater than 0.");
        }
    }

    private void _pull(int batchSize, boolean noWait, Duration expiresIn) {
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
    public Iterator<Message> iterate(int batchSize, Duration maxWait) {
        return iterate(batchSize, maxWait.toMillis());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Message> iterate(final int batchSize, long maxWaitMillis) {
        pullNoWait(batchSize);

        return new Iterator<Message>() {
            int received = 0;
            long timeLeft = Long.MAX_VALUE;
            Message msg = null;

            @Override
            public boolean hasNext() {
                try {
                    if (msg == null) {
                        if (timeLeft < 1) { // msg is null and no more time
                            return false;
                        }

                        // first time check. Did not want to do it on construction
                        // of iterator, waited until first hasNext call
                        // this does 2 things. Gives the internal queue time to fill
                        // as saves the full wait time until the user actually calls hasNext
                        if (timeLeft == Long.MAX_VALUE) {
                            timeLeft = maxWaitMillis;
                        }

                        long endTime = System.currentTimeMillis() + timeLeft;
                        msg = nextMessageWithEndTime(endTime);
                        if (msg == null) {
                            timeLeft = 0;
                            return false;
                        }

                        // msg is not null
                        if (++received == batchSize) {
                            timeLeft = 0; // don't need any more time, got entire batch
                        }
                        else {
                            timeLeft = endTime - System.currentTimeMillis();
                        }
                    }
                    // else message was not null, I guess they called hasNext multiple times w/o next
                    return true;
                } catch (InterruptedException e) {
                    msg = null;
                    timeLeft = 0;
                    Thread.currentThread().interrupt();
                    return false;
                }
            }

            @Override
            public Message next() {
                Message next = msg;
                msg = null;
                return next;
            }
        };
    }
}
