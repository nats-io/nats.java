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
import java.util.concurrent.atomic.AtomicBoolean;

import io.nats.client.Dispatcher;
import io.nats.client.MessageHandler;

class NatsDispatcher implements Dispatcher, Runnable {

    private NatsConnection connection;
    private MessageQueue incoming;
    private MessageHandler handler;

    private Thread thread;
    private final AtomicBoolean running;

    private Map<String, NatsSubscription> subscriptions;

    NatsDispatcher(NatsConnection conn, MessageHandler handler) {
        this.connection = conn;
        this.handler = handler;
        this.incoming = new MessageQueue(true);
        this.subscriptions = new ConcurrentHashMap<>();
        this.running = new AtomicBoolean(false);
    }

    void start() {
        this.running.set(true);
        this.thread = new Thread(this);
        this.thread.start();
    }

    public void run() {
        Duration waitForMessage = Duration.ofMinutes(5); // This can be long since we aren't doing anything

        try {
            while (this.running.get()) {
                NatsMessage msg = this.incoming.pop(waitForMessage);

                if (msg == null) {
                    continue;
                }

                NatsSubscription sub = msg.getNatsSubscription();

                if (sub != null && sub.isActive()) {
                    try {
                        handler.onMessage(msg);
                    } catch (Exception exp) {
                        this.connection.processException(exp);
                    }
                    
                    sub.incrementMessageCount();

                    if (sub.reachedMax()) {
                        this.connection.invalidate(sub);
                    }
                }
            }
        } catch (InterruptedException exp) {
            this.connection.processException(exp);
        } finally {
            this.running.set(false);
            this.thread = null;
        }
    }

    void stop() {
        this.running.set(false);
        this.incoming.pause();
        this.subscriptions.clear();
    }

    MessageQueue getMessageQueue() {
        return incoming;
    }

    void resendSubscriptions() {
        this.subscriptions.forEach((id, sub)->{
            this.connection.sendSubscriptionMessage(sub.getSID(), sub.getSubject(), sub.getQueueName());
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

        NatsSubscription sub = subscriptions.get(subject);

        if (sub == null) {
            sub = connection.createSubscription(subject, null, this);
            NatsSubscription actual = subscriptions.putIfAbsent(subject, sub);
            if (actual != null) {
                this.connection.unsubscribe(sub, -1); // Could happen on very bad timing
            }
        }

        return this;
    }

    public Dispatcher subscribe(String subject, String queueName) {
        if (subject == null || subject.length() == 0) {
            throw new IllegalArgumentException("Subject is required in subscribe");
        }

        if (queueName == null || queueName.length() == 0) {
            throw new IllegalArgumentException("QueueName is required in subscribe");
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
        if (subject == null || subject.length() == 0) {
            throw new IllegalArgumentException("Subject is required in unsubscribe");
        }
        
        NatsSubscription sub = subscriptions.get(subject);

        if (sub != null) {
            this.connection.unsubscribe(sub, after); // Connection will tell us when to remove from the map
        }

        return this;
    }

}