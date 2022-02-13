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
import java.util.Collections;
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
    public void pullNoWait(int batchSize, Duration expiresIn) {
        durationGtZeroRequired(expiresIn, "NoWait");
        _pull(batchSize, true, expiresIn);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullNoWait(int batchSize, long expiresInMillis) {
        durationGtZeroRequired(expiresInMillis, "NoWait");
        _pull(batchSize, true, Duration.ofMillis(expiresInMillis));
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
        durationGtZeroRequired(expiresInMillis, "Expires In");
        _pull(batchSize, false, Duration.ofMillis(expiresInMillis));
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
    public List<Message> fetch(int batchSize, long maxWaitMillis) {
        durationGtZeroRequired(maxWaitMillis, "Fetch");
        return _fetch(batchSize, Duration.ofMillis(maxWaitMillis), maxWaitMillis);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Message> fetch(int batchSize, Duration maxWait) {
        durationGtZeroRequired(maxWait, "Fetch");
        return _fetch(batchSize, maxWait, maxWait.toMillis());
    }

    private List<Message> _fetch(int batchSize, Duration maxWait, Long maxWaitMillis) {
        try {
            _pull(batchSize, false, maxWait);
            return _nextUnmanaged(batchSize, maxWaitMillis);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Collections.emptyList();
        }
    }

    private void durationGtZeroRequired(Duration duration, String label) {
        if (duration == null || duration.toMillis() <= 0) {
            throw new IllegalArgumentException(label + " wait duration must be supplied and greater than 0.");
        }
    }

    private void durationGtZeroRequired(long millis, String label) {
        if (millis <= 0) {
            throw new IllegalArgumentException(label + " wait duration must be supplied and greater than 0.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Message> iterate(int batchSize, Duration maxWait) {
        durationGtZeroRequired(maxWait, "Iterate");
        return _iterate(batchSize, maxWait, maxWait.toMillis());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Message> iterate(final int batchSize, long maxWaitMillis) {
        durationGtZeroRequired(maxWaitMillis, "Iterate");
        return _iterate(batchSize, Duration.ofMillis(maxWaitMillis), maxWaitMillis);
    }

    private Iterator<Message> _iterate(final int batchSize, Duration maxWait, long maxWaitMillis) {
        _pull(batchSize, false, maxWait);

        return new Iterator<Message>() {
            int received = 0;
            boolean done = false;
            Message msg = null;

            @Override
            public boolean hasNext() {
                try {
                    if (msg != null) {
                        return true;
                    }

                    if (done) {
                        return false;
                    }

                    msg = _nextUnmanaged(maxWaitMillis);
                    if (msg == null) {
                        done = true;
                        return false;
                    }

                    done = ++received == batchSize;
                    return true;
                }
                catch (InterruptedException e) {
                    msg = null;
                    done = true;
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
