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
import io.nats.client.support.NatsJetStreamConstants;

import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;

import static io.nats.client.impl.MessageManager.ManageResult.MESSAGE;

/**
 * This is a JetStream specific subscription.
 */
public class NatsJetStreamSubscription extends NatsSubscription implements JetStreamSubscription, NatsJetStreamConstants {

    public static final String SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL = "Subscription type does not support pull.";
    public static final long EXPIRE_ADJUSTMENT = 10;
    public static final long MIN_EXPIRE_MILLIS = 20;

    protected final NatsJetStream js;

    protected String stream;
    protected String consumerName;

    protected MessageManager manager;

    NatsJetStreamSubscription(String sid, String subject, String queueName,
                              NatsConnection connection, NatsDispatcher dispatcher,
                              NatsJetStream js,
                              String stream, String consumer,
                              MessageManager manager)
    {
        super(sid, subject, queueName, connection, dispatcher);

        this.js = js;
        this.stream = stream;
        this.consumerName = consumer; // might be null, someone will call setConsumerName

        this.manager = manager;
        manager.startup(this);
    }

    void setConsumerName(String consumerName) {
        this.consumerName = consumerName;
    }

    @Override
    public String getConsumerName() {
        return consumerName;
    }

    String getStream() {
        return stream;
    }

    boolean isPullMode() {
        return false;
    }

    MessageManager getManager() { return manager; } // internal, for testing

    @Override
    void invalidate() {
        manager.shutdown();
        super.invalidate();
    }

    @Override
    boolean isDrained() {
        if (!isDraining()) {
            return false;
        }

        // could contain status messages, but those don't count towards being drained
        if (this.getPendingMessageCount() > 0) {
            MessageQueue mq = getMessageQueue();
            for (Message msg : mq.queue) {
                if (msg == mq.poisonPill) break;
                // if there's at least one message, we are not drained yet
                if (manager.manage(msg) == MESSAGE) return false;
            }
        }
        return true;
    }

    @Override
    public Message nextMessage(Duration timeout) throws InterruptedException, IllegalStateException {
        if (timeout == null) {
            return _nextUnmanagedNoWait(null);
        }
        long millis = timeout.toMillis();
        if (millis <= 0) {
            return _nextUnmanagedWaitForever(null);
        }
        return _nextUnmanaged(millis, null);
    }

    @Override
    public Message nextMessage(long timeoutMillis) throws InterruptedException, IllegalStateException {
        if (timeoutMillis <= 0) {
            return _nextUnmanagedWaitForever(null);
        }
        return _nextUnmanaged(timeoutMillis, null);
    }

    protected Message _nextUnmanagedWaitForever(String expectedPullSubject) throws InterruptedException {
        while (true) {
            Message msg = nextMessageInternal(Duration.ZERO);
            if (msg != null) { // null shouldn't happen, so just a code guard b/c nextMessageInternal can return null
                switch (manager.manage(msg)) {
                    case MESSAGE:
                        return msg;
                    case STATUS_ERROR:
                        // if the status applies throw exception, otherwise it's ignored, fall through
                        if (expectedPullSubject == null || expectedPullSubject.equals(msg.getSubject())) {
                            throw new JetStreamStatusException(msg.getStatus(), this);
                        }
                        break;
                }
                // Check again since waiting forever when:
                // 1. Any STATUS_HANDLED or STATUS_TERMINUS
                // 2. STATUS_ERRORS that aren't for expected pullSubject
            }
        }
    }

    protected Message _nextUnmanagedNoWait(String expectedPullSubject) throws InterruptedException {
        while (true) {
            Message msg = nextMessageInternal(null);
            if (msg == null) {
                return null;
            }
            switch (manager.manage(msg)) {
                case MESSAGE:
                    return msg;
                case STATUS_TERMINUS:
                    // if the status applies return null, otherwise it's ignored, fall through
                    if (expectedPullSubject == null || expectedPullSubject.equals(msg.getSubject())) {
                        return null;
                    }
                    break;
                case STATUS_ERROR:
                    // if the status applies throw exception, otherwise it's ignored, fall through
                    if (expectedPullSubject == null || expectedPullSubject.equals(msg.getSubject())) {
                        throw new JetStreamStatusException(msg.getStatus(), this);
                    }
                    break;
            }
            // Check again when, regular messages might have arrived
            // 1. Any STATUS_HANDLED
            // 2. STATUS_TERMINUS or STATUS_ERRORS that aren't for expected pullSubject
        }
    }

    protected Message _nextUnmanaged(long timeout, String expectedPullSubject) throws InterruptedException {
        long timeoutNanos = timeout * 1_000_000;
        long timeLeftNanos = timeoutNanos;
        long start = System.nanoTime();
        while (timeLeftNanos > 0) {
            Message msg = nextMessageInternal( Duration.ofNanos(timeLeftNanos) );
            if (msg == null) {
                return null; // normal timeout
            }
            switch (manager.manage(msg)) {
                case MESSAGE:
                    return msg;
                case STATUS_TERMINUS:
                    // if the status applies return null, otherwise it's ignored, fall through
                    if (expectedPullSubject == null || expectedPullSubject.equals(msg.getSubject())) {
                        return null;
                    }
                    break;
                case STATUS_ERROR:
                    // if the status applies throw exception, otherwise it's ignored, fall through
                    if (expectedPullSubject == null || expectedPullSubject.equals(msg.getSubject())) {
                        throw new JetStreamStatusException(msg.getStatus(), this);
                    }
                    break;
            }
            // anything else, try again while we have time
            timeLeftNanos = timeoutNanos - (System.nanoTime() - start);
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pull(int batchSize) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pull(PullRequestOptions pullRequestOptions) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullNoWait(int batchSize) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullNoWait(int batchSize, Duration expiresIn) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullNoWait(int batchSize, long expiresInMillis) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullExpiresIn(int batchSize, Duration expiresIn) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullExpiresIn(int batchSize, long expiresInMillis) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Message> fetch(int batchSize, long maxWaitMillis) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Message> fetch(int batchSize, Duration maxWait) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Message> iterate(int batchSize, Duration maxWait) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Message> iterate(final int batchSize, long maxWaitMillis) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JetStreamReader reader(int batchSize, int repullAt) {
        throw new IllegalStateException(SUBSCRIPTION_TYPE_DOES_NOT_SUPPORT_PULL);
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
                ", deliver='" + getSubject() + '\'' +
                ", isPullMode=" + isPullMode() +
                '}';
    }
}
