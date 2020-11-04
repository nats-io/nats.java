// Copyright 2015-2018 The NATS Authors
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

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import io.nats.client.Subscription;
import io.nats.client.Dispatcher;
import io.nats.client.MessageHandler;

class NatsDispatcher extends NatsConsumer implements Dispatcher, Runnable {

    private MessageQueue incoming;
    private MessageHandler defaultHandler;

    private Future<Boolean> thread;
    private final AtomicBoolean running;

    private ByteBuffer id;

    // We will use the subject as the key for subscriptions that use the
    // default handler.
    private Map<ByteBuffer, NatsSubscription> subscriptionsUsingDefaultHandler;
    // We will use the SID as the key. Sicne these subscriptions provide
    // their own handlers, we allow duplicates. There is a subtle but very
    // important difference here.
    private Map<ByteBuffer, NatsSubscription> subscriptionsWithHandlers;
    // We use the SID as the key here.
    private Map<ByteBuffer, MessageHandler> subscriptionHandlers;

    private Duration waitForMessage;


    NatsDispatcher(NatsConnection conn, MessageHandler handler) {
        super(conn);
        this.defaultHandler = handler;
        this.incoming = new MessageQueue(true);
        this.subscriptionsUsingDefaultHandler = new ConcurrentHashMap<>();
        this.subscriptionsWithHandlers = new ConcurrentHashMap<>();
        this.subscriptionHandlers = new ConcurrentHashMap<>();
        this.running = new AtomicBoolean(false);
        this.waitForMessage = Duration.ofMinutes(5); // This can be long since we aren't doing anything
    }

    void start(ByteBuffer id) {
        this.id = id;
        this.running.set(true);
        thread = connection.getExecutor().submit(this, Boolean.TRUE);
    }

    boolean breakRunLoop() {
        return this.incoming.isDrained();
    }

    public void run() {
        try {
            while (this.running.get()) {

                NatsMessage msg = this.incoming.pop(this.waitForMessage);

                if (msg == null) {
                    if (breakRunLoop()) {
                        return;
                    } else {
                        continue;
                    }
                }

                NatsSubscription sub = msg.getNatsSubscription();

                if (sub != null && sub.isActive()) {

                    sub.incrementDeliveredCount();
                    this.incrementDeliveredCount();

                    MessageHandler currentHandler = this.defaultHandler;
                    MessageHandler customHandler = this.subscriptionHandlers.get(sub.getSIDBuffer());
                    if (customHandler != null) {
                        currentHandler = customHandler;
                    }

                    try {
                        currentHandler.onMessage(msg);
                    } catch (Exception exp) {
                        this.connection.processException(exp);
                    }

                    if (sub.reachedUnsubLimit()) {
                        this.connection.invalidate(sub);
                    }
                }

                if (breakRunLoop()) {
                    // will set the dispatcher to not active
                    return;
                }
            }
        } catch (InterruptedException exp) {
            if (this.running.get()){
                this.connection.processException(exp);
            } //otherwise we did it
        } finally {
            this.running.set(false);
            this.thread = null;
        }
    }

    void stop(boolean unsubscribeAll) {
        this.running.set(false);
        this.incoming.pause();

        if (this.thread != null) {
            try {
                if (!this.thread.isCancelled()) {
                    this.thread.cancel(true);
                }
            } catch (Exception exp) {
                // let it go
            }
        }

        if (unsubscribeAll) {
            this.subscriptionsUsingDefaultHandler.forEach((subj, sub) -> {
                this.connection.unsubscribe(sub, -1);
            });
            this.subscriptionsWithHandlers.forEach((sid, sub) -> {
                this.connection.unsubscribe(sub, -1);
            });
        } else {
            this.subscriptionsUsingDefaultHandler.clear();
            this.subscriptionsWithHandlers.clear();
            this.subscriptionHandlers.clear();
        }
    }

    public boolean isActive() {
        return this.running.get();
    }

    ByteBuffer getId() {
        return id.duplicate();
    }

    MessageQueue getMessageQueue() {
        return incoming;
    }

    void resendSubscriptions() {
        this.subscriptionsUsingDefaultHandler.forEach((id, sub)->{
            this.connection.sendSubscriptionMessage(sub.getSIDBuffer(), sub.getSubjectBuffer(), sub.getQueueNameBuffer(), true);
        });
        this.subscriptionsWithHandlers.forEach((sid, sub)->{
            this.connection.sendSubscriptionMessage(sub.getSIDBuffer(), sub.getSubjectBuffer(), sub.getQueueNameBuffer(), true);
        });
    }

    // Called by the connection when a subscription is removed.
    // We will first attempt to remove from subscriptionsWithHandlers
    // using the sub's SID, and if we don't find it there, we'll check
    // the subscriptionsUsingDefaultHandler Map and verify the SID
    // matches before removing. By verifying the SID in all cases we can
    // be certain we're removing the correct Subscription.
    void remove(NatsSubscription sub) {
        if (this.subscriptionsWithHandlers.remove(sub.getSIDBuffer()) != null) {
            this.subscriptionHandlers.remove(sub.getSIDBuffer());
        } else {
            NatsSubscription s = this.subscriptionsUsingDefaultHandler.get(sub.getSubjectBuffer());
            if (s.getSIDBuffer().equals(sub.getSIDBuffer())) {
                this.subscriptionsUsingDefaultHandler.remove(sub.getSubjectBuffer());
            }
        }
    }

    public Dispatcher subscribe(ByteBuffer subject) {
        if (subject == null || subject.remaining() == 0) {
            throw new IllegalArgumentException("Subject is required in subscribe");
        }

        this.subscribeImpl(subject, null, null);
        return this;
    }

    public Dispatcher subscribe(String subject) {
        if (subject == null || subject.length() == 0) {
            throw new IllegalArgumentException("Subject is required in subscribe");
        }

        this.subscribeImpl(subject, null, null);
        return this;
    }
    NatsSubscription subscribeReturningSubscription(ByteBuffer subject) {
        if (subject == null || subject.limit() == 0) {
            throw new IllegalArgumentException("Subject is required in subscribe");
        }

        return this.subscribeImpl(subject, null, null);
    }
    NatsSubscription subscribeReturningSubscription(String subject) {
        return subscribeReturningSubscription(NatsEncoder.encodeSubject(subject));
    }
    public Subscription subscribe(String subject, MessageHandler handler) {
        if (subject == null || subject.length() == 0) {
            throw new IllegalArgumentException("Subject is required in subscribe");
        }

        if (handler == null) {
            throw new IllegalArgumentException("MessageHandler is required in subscribe");
        }
        return this.subscribeImpl(subject, null, handler);
    }

    public Dispatcher subscribe(String subject, String queueName) {
        if (subject == null || subject.length() == 0) {
            throw new IllegalArgumentException("Subject is required in subscribe");
        }

        if (queueName == null || queueName.length() == 0) {
            throw new IllegalArgumentException("QueueName is required in subscribe");
        }
        this.subscribeImpl(subject, queueName, null);
        return this;
    }

    public Subscription subscribe(String subject, String queueName,  MessageHandler handler) {
        if (subject == null || subject.length() == 0) {
            throw new IllegalArgumentException("Subject is required in subscribe");
        }

        if (queueName == null || queueName.length() == 0) {
            throw new IllegalArgumentException("QueueName is required in subscribe");
        }

        if (handler == null) {
            throw new IllegalArgumentException("MessageHandler is required in subscribe");
        }
        return this.subscribeImpl(subject, queueName, handler);
    }

    // Assumes the subj/queuename checks are done, does check for closed status
    NatsSubscription subscribeImpl(String subject, String queueName, MessageHandler handler) {
        ByteBuffer subjectBuf = null;
        if (subject != null)
            subjectBuf = NatsEncoder.encodeSubject(subject);

        ByteBuffer queueNameBuf = null;
        if (queueName != null)
            queueNameBuf = NatsEncoder.encodeQueue(queueName);

        return subscribeImpl(subjectBuf, queueNameBuf, handler);
    }

    // Assumes the subj/queuename checks are done, does check for closed status
    NatsSubscription subscribeImpl(ByteBuffer subject, ByteBuffer queueName, MessageHandler handler) {
        if (!this.running.get()) {
            throw new IllegalStateException("Dispatcher is closed");
        }

        if (this.isDraining()) {
            throw new IllegalStateException("Dispatcher is draining");
        }

        // If the handler is null, then we use the default handler, which will not allow
        // duplicate subscriptions to exist.
        if (handler == null) {
            NatsSubscription sub = this.subscriptionsUsingDefaultHandler.get(subject);

            if (sub == null) {
                sub = connection.createSubscription(subject, queueName, this);
                NatsSubscription actual = this.subscriptionsUsingDefaultHandler.putIfAbsent(subject, sub);
                if (actual != null) {
                    this.connection.unsubscribe(sub, -1); // Could happen on very bad timing
                }
            }

            return sub;
        } else {
            NatsSubscription sub = connection.createSubscription(subject, queueName, this);
            this.subscriptionsWithHandlers.put(sub.getSIDBuffer(), sub);
            this.subscriptionHandlers.put(sub.getSIDBuffer(), handler);
            return sub;
        }
    }

    public Dispatcher unsubscribe(String subject) {
        return this.unsubscribe(subject, -1);
    }

    public Dispatcher unsubscribe(ByteBuffer subject) {
        return this.unsubscribe(subject, -1);
    }

    public Dispatcher unsubscribe(Subscription subscription) {
        return this.unsubscribe(subscription, -1);
    }

    public Dispatcher unsubscribe(String subject, int after) {
        return this.unsubscribe(
                NatsEncoder.encodeSubject(subject),
                after
        );
    }

    public Dispatcher unsubscribe(ByteBuffer subject, int after) {
        if (!this.running.get()) {
            throw new IllegalStateException("Dispatcher is closed");
        }

        if (isDraining()) { // No op while draining
            return this;
        }

        if (subject == null || subject.remaining() == 0) {
            throw new IllegalArgumentException("Subject is required in unsubscribe");
        }

        NatsSubscription sub = this.subscriptionsUsingDefaultHandler.get(subject);

        if (sub != null) {
            this.connection.unsubscribe(sub, after); // Connection will tell us when to remove from the map
        }

        return this;
    }

    public Dispatcher unsubscribe(Subscription subscription, int after) {
        if (!this.running.get()) {
            throw new IllegalStateException("Dispatcher is closed");
        }

        if (isDraining()) { // No op while draining
            return this;
        }

        if (subscription.getDispatcher() != this) {
            throw new IllegalStateException("Subscription is not managed by this Dispatcher");
        }

        // We can probably optimize this path by adding getSID() to the Subscription interface.
        if (!(subscription instanceof NatsSubscription)) {
            throw new IllegalArgumentException("This Subscription implementation is not known by Dispatcher");
        }
        
        NatsSubscription ns = ((NatsSubscription) subscription);
        // Grab the NatsSubscription to verify we weren't given a different manager's subscription.
        NatsSubscription sub = this.subscriptionsWithHandlers.get(ns.getSIDBuffer());

        if (sub != null) {
            this.connection.unsubscribe(sub, after); // Connection will tell us when to remove from the map
        }

        return this;
    }

    void sendUnsubForDrain() {
        this.subscriptionsUsingDefaultHandler.forEach((id, sub)->{
            this.connection.sendUnsub(sub, -1);
        });
        this.subscriptionsWithHandlers.forEach((sid, sub)->{
            this.connection.sendUnsub(sub, -1);
        });
    }

    void cleanUpAfterDrain() {
        this.connection.cleanupDispatcher(this);
    }

    public boolean isDrained() {
        return !isActive() && super.isDrained();
    }
}
