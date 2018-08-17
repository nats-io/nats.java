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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.nats.client.Consumer;

abstract class NatsConsumer implements Consumer {

    NatsConnection connection;
    private AtomicLong maxMessages;
    private AtomicLong maxBytes;
    private AtomicLong droppedMessages;
    private AtomicLong messagesDelivered;
    private AtomicBoolean slow;
    private AtomicReference<CompletableFuture<Boolean>> draining;

    NatsConsumer(NatsConnection conn) {
        this.connection = conn;
        this.maxMessages = new AtomicLong();
        this.maxBytes = new AtomicLong();
        this.droppedMessages = new AtomicLong();
        this.messagesDelivered = new AtomicLong(0);
        this.slow = new AtomicBoolean(false);
        this.draining = new AtomicReference<>();
    }

    /**
     * Set limits on the maximum number of messages, or maximum size of messages this consumer
     * will hold before it starts to drop new messages waiting for the application to drain the queue.
     * 
     * <p>Messages are dropped as they encounter a full queue, which is to say, new messages are dropped rather than
     * old messages. If a queue is 10 deep and fills up, the 11th message is dropped.
     * 
     * @param maxMessages the maximum message count to hold, defaults to {{@value #DEFAULT_MAX_MESSAGES}}.
     * @param maxBytes the maximum bytes to hold, defaults to {{@value #DEFAULT_MAX_BYTES}}.
     */
    public void setPendingLimits(long maxMessages, long maxBytes) {
        this.maxMessages.set(maxMessages);
        this.maxBytes.set(maxBytes);
    }

    /**
     * @return the pending message limit set by {@link #setPendingLimits(long, long) setPendingLimits}.
     */
    public long getPendingMessageLimit() {
        return this.maxMessages.get();
    }

    /**
     * @return the pending byte limit set by {@link #setPendingLimits(long, long) setPendingLimits}.
     */
    public long getPendingByteLimit() {
        return this.maxBytes.get();
    }

    /**
     * @return the number of messages waiting to be delivered/popped, {@link #setPendingLimits(long, long) setPendingLimits}.
     */
    public long getPendingMessageCount() {
        return this.getMessageQueue()!=null ? this.getMessageQueue().length() : 0;
    }

    /**
     * @return the cumulative size of the messages waiting to be delivered/popped, {@link #setPendingLimits(long, long) setPendingLimits}.
     */
    public long getPendingByteCount() {
        return this.getMessageQueue()!=null ? this.getMessageQueue().sizeInBytes() : 0;
    }

    /**
     * @return the total number of messages delivered to this consumer, for all time.
     */
    public long getDeliveredCount() {
        return this.messagesDelivered.get();
    }

    void incrementDeliveredCount() {
        this.messagesDelivered.incrementAndGet();
    }

    void incrementDroppedCount() {
        this.droppedMessages.incrementAndGet();
    }

    /**
     * @return the number of messages dropped from this consumer, since the last call to {@link @clearDroppedCount}.
     */
    public long getDroppedCount() {
        return this.droppedMessages.get();
    }

    /**
     * Reset the drop count to 0.
     */
    public void clearDroppedCount() {
        this.droppedMessages.set(0);
    }

    void markSlow() {
        this.slow.set(true);
    }

    void markNotSlow() {
        this.slow.set(false);
    }

    boolean isMarkedSlow() {
        return this.slow.get();
    }

    void markDraining(CompletableFuture<Boolean> future) {
        this.draining.set(future);
    }

    CompletableFuture<Boolean> getDrainingFuture() {
        return this.draining.get();
    }

    boolean isDraining() {
        return this.draining.get() != null;
    }

    boolean hasReachedPendingLimits() {
        return ((this.getPendingByteCount() >= this.getPendingByteLimit() && this.getPendingByteLimit() > 0) ||
                    (this.getPendingMessageCount() >= this.getPendingMessageLimit() && this.getPendingMessageLimit() > 0));
    }

    boolean isDrained() {
        return isDraining() && this.getPendingMessageCount() == 0;
    }

    /**
    * Drain tells the consumer to process in flight, or cached messages, but stop receiving new ones. The library will
    * flush the unsubscribe call(s) insuring that any publish calls made by this client are included. When all messages
    * are processed the consumer effectively becomes unsubscribed.
    * 
    * @param timeout The time to wait for the drain to succeed, pass 0 to wait
    *                    forever. Drain involves moving messages to and from the server
    *                    so a very short timeout is not recommended.
    * @return A future that can be used to check if the drain has completed
    */
   public CompletableFuture<Boolean> drain(Duration timeout) {
       if (!this.isActive()) {
           throw new IllegalStateException("Consumer is closed");
       }

       if (isDraining()) {
           return this.getDrainingFuture();
       }

       this.markDraining(this.connection.drain(this, timeout));
       this.sendUnsubToConnection();
       return getDrainingFuture();
   }

    /**
     * @return whether or not this consumer is still processing messages.
     * For a subscription the answer is false after unsubscribe. For a dispatcher, false after stop.
     */
    public abstract boolean isActive();

    abstract MessageQueue getMessageQueue();

    /**
     * Called during drain to tell the consumer to send appropriate unsub requests to the connection.
     * 
     * A subscription will unsub itself, while a dispatcher will unsub all of its subscriptions.
     */
    abstract void sendUnsubToConnection();

    /**
     * Abstract method, called by the connection when the drain is complete.
     */
    abstract void cleanUpAfterDrain(Duration timeout);
}