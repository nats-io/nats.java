// Copyright 2021-2022 The NATS Authors
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

import io.nats.client.JetStreamReader;
import io.nats.client.Message;
import io.nats.client.PullRequestOptions;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class NatsJetStreamPullSubscription extends NatsJetStreamSubscription {

    NatsJetStreamPullSubscription(String sid, String subject,
                                  NatsConnection connection,
                                  NatsDispatcher dispatcher,
                                  NatsJetStream js,
                                  String stream, String consumer,
                                  MessageManager[] managers) {
        super(sid, subject, null, connection, dispatcher, js, stream, consumer, managers);
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
        pull(PullRequestOptions.builder(batchSize).build());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pull(PullRequestOptions pullRequestOptions) {
        String publishSubject = js.prependPrefix(String.format(JSAPI_CONSUMER_MSG_NEXT, stream, consumerName));
        connection.publish(publishSubject, getSubject(), pullRequestOptions.serialize());
        connection.lenientFlushBuffer();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullNoWait(int batchSize) {
        pull(PullRequestOptions.noWait(batchSize).build());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullNoWait(int batchSize, Duration expiresIn) {
        durationGtZeroRequired(expiresIn, "NoWait Expires In");
        pull(PullRequestOptions.noWait(batchSize).expiresIn(expiresIn).build());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullNoWait(int batchSize, long expiresInMillis) {
        durationGtZeroRequired(expiresInMillis, "NoWait Expires In");
        pull(PullRequestOptions.noWait(batchSize).expiresIn(expiresInMillis).build());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullExpiresIn(int batchSize, Duration expiresIn) {
        durationGtZeroRequired(expiresIn, "Expires In");
        pull(PullRequestOptions.builder(batchSize).expiresIn(expiresIn).build());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullExpiresIn(int batchSize, long expiresInMillis) {
        durationGtZeroRequired(expiresInMillis, "Expires In");
        pull(PullRequestOptions.builder(batchSize).expiresIn(expiresInMillis).build());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Message> fetch(int batchSize, long maxWaitMillis) {
        durationGtZeroRequired(maxWaitMillis, "Fetch");
        return _fetch(batchSize, maxWaitMillis);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Message> fetch(int batchSize, Duration maxWait) {
        durationGtZeroRequired(maxWait, "Fetch");
        return _fetch(batchSize, maxWait.toMillis());
    }

    private List<Message> _fetch(int batchSize, long maxWaitMillis) {
        List<Message> messages = drainAlreadyBuffered(batchSize);

        int batchLeft = batchSize - messages.size();
        if (batchLeft == 0) {
            return messages;
        }

        try {
            long start = System.currentTimeMillis();

            Duration expires = Duration.ofMillis(
                maxWaitMillis > MIN_MILLIS
                    ? maxWaitMillis - EXPIRE_LESS_MILLIS
                    : maxWaitMillis);
            pull(PullRequestOptions.builder(batchLeft).expiresIn(expires).build());

            // timeout > 0 process as many messages we can in that time period
            // If we get a message that either manager handles, we try again, but
            // with a shorter timeout based on what we already used up
            long timeLeft = maxWaitMillis;
            while (batchLeft > 0 && timeLeft > 0) {
                Message msg = nextMessageInternal( Duration.ofMillis(timeLeft) );
                if (msg == null) {
                    return messages; // normal timeout
                }
                if (!anyManaged(msg)) { // not null and not managed means JS Message
                    messages.add(msg);
                    batchLeft--;
                }
                // try again while we have time
                timeLeft = maxWaitMillis - (System.currentTimeMillis() - start);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return messages;
    }

    private List<Message> drainAlreadyBuffered(int batchSize) {
        List<Message> messages = new ArrayList<>(batchSize);
        try {
            Message msg = nextMessageInternal(null); // null means do not wait, it's either already here or not
            while (msg != null) {
                if (!anyManaged(msg)) { // not null and not managed means JS Message
                    messages.add(msg);
                    if (messages.size() == batchSize) {
                        return messages;
                    }
                }
                msg = nextMessageInternal(null);
            }
        }
        catch (InterruptedException ignore) {
            // shouldn't ever happen in reality
        }
        return messages;
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
        return _iterate(batchSize, maxWait.toMillis());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Message> iterate(final int batchSize, long maxWaitMillis) {
        durationGtZeroRequired(maxWaitMillis, "Iterate");
        return _iterate(batchSize, maxWaitMillis);
    }

    private Iterator<Message> _iterate(final int batchSize, long maxWaitMillis) {
        final List<Message> buffered = drainAlreadyBuffered(batchSize);

        // if there was a full batch buffered, no need to pull, just iterate over the list you already have
        int batchLeft = batchSize - buffered.size();
        if (batchLeft == 0) {
            return new Iterator<Message>() {
                @Override
                public boolean hasNext() {
                    return buffered.size() > 0;
                }

                @Override
                public Message next() {
                    return buffered.remove(0);
                }
            };
        }

        // if there were some messages buffered, reduce the raw pull batch size
        pull(PullRequestOptions.builder(batchLeft).expiresIn(maxWaitMillis).build());

        final long timeout = maxWaitMillis;

        // the iterator is also more complicated
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

                    if (buffered.size() == 0) {
                        msg = _nextUnmanaged(timeout);
                        if (msg == null) {
                            done = true;
                            return false;
                        }
                    }
                    else {
                        msg = buffered.remove(0);
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

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamReader reader(final int batchSize, final int repullAt) {
        return new NatsJetStreamPullReader(this, batchSize, repullAt);
    }
}
