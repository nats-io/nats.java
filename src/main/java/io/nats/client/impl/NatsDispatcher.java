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

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import io.nats.client.Dispatcher;
import io.nats.client.MessageHandler;

class NatsDispatcher extends NatsConsumer implements Dispatcher, Runnable {

    private MessageQueue incoming;
    private MessageHandler handler;

    private Future<Boolean> thread;
    private final AtomicBoolean running;

    private String id;

    private Map<String, NatsSubscription> subscriptions;
    private Duration waitForMessage;


    NatsDispatcher(NatsConnection conn, MessageHandler handler) {
        super(conn);
        this.handler = handler;
        this.incoming = new MessageQueue(true);
        this.subscriptions = new ConcurrentHashMap<>();
        this.running = new AtomicBoolean(false);
        this.waitForMessage = Duration.ofMinutes(5); // This can be long since we aren't doing anything
    }

    void start(String id) {
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

                    try {
                        handler.onMessage(msg);
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
            this.subscriptions.forEach((subj, sub) -> {
                this.connection.unsubscribe(sub, -1);
            });
        } else {
            this.subscriptions.clear();
        }
    }

    public boolean isActive() {
        return this.running.get();
    }

    String getId() {
        return id;
    }

    MessageQueue getMessageQueue() {
        return incoming;
    }

    void resendSubscriptions() {
        this.subscriptions.forEach((id, sub)->{
            this.connection.sendSubscriptionMessage(sub.getSID(), sub.getSubject(), sub.getQueueName(), true);
        });
    }

    // Called by the connection when the subscription is removed
    void remove(NatsSubscription sub) {
        subscriptions.remove(sub.getSubject());
    }

    public Dispatcher subscribe(String subject) {
        if (subject == null || subject.length() == 0) {
            throw new IllegalArgumentException("Subject is required in subscribe");
        }

        return this.subscribeImpl(subject, null);
    }

    public Dispatcher subscribe(String subject, String queueName) {
        if (subject == null || subject.length() == 0) {
            throw new IllegalArgumentException("Subject is required in subscribe");
        }

        if (queueName == null || queueName.length() == 0) {
            throw new IllegalArgumentException("QueueName is required in subscribe");
        }
        return this.subscribeImpl(subject, queueName);
    }


    // Assumes the subj/queuename checks are done, does check for closed status
    Dispatcher subscribeImpl(String subject, String queueName) {
        if (!this.running.get()) {
            throw new IllegalStateException("Dispatcher is closed");
        }
        
        if (this.isDraining()) {
            throw new IllegalStateException("Dispatcher is draining");
        }

        NatsSubscription sub = subscriptions.get(subject);

        if (sub == null) {
            sub = connection.createSubscription(subject, queueName, this);
            NatsSubscription actual = subscriptions.putIfAbsent(subject, sub);
            if (actual != null) {
                this.connection.unsubscribe(sub, -1); // Could happen on very bad timing
            }
        }

        return this;
    }

    public Dispatcher unsubscribe(String subject) {
        return this.unsubscribe(subject, -1);
    }

    public Dispatcher unsubscribe(String subject, int after) {
        if (!this.running.get()) {
            throw new IllegalStateException("Dispatcfher is closed");
        }

        if (isDraining()) { // No op while draining
            return this;
        }

        if (subject == null || subject.length() == 0) {
            throw new IllegalArgumentException("Subject is required in unsubscribe");
        }
        
        NatsSubscription sub = subscriptions.get(subject);

        if (sub != null) {
            this.connection.unsubscribe(sub, after); // Connection will tell us when to remove from the map
        }

        return this;
    }

    void sendUnsubForDrain() {
        this.subscriptions.forEach((id, sub)->{
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