/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

/**
 * A client uses an {@code AsyncSubscription} object to receive messages from a subject. It runs
 * asynchronously (on an {@code Executor}), receives one {@code Message} at a time, and invokes a
 * user-defined callback method to process each {@code Message}.
 * 
 */
public interface AsyncSubscription extends Subscription {

    /**
     * Starts asynchronous message delivery to this subscription
     * 
     * @throws IllegalStateException if the subscription is invalid/closed. Reasons for this could
     *         include reaching the subscription's autoUnsubscribe limit, calling
     *         {@link #unsubscribe()}, or closing the Connection.
     */
    void start();

    /**
     * @param cb the message handler to set for this subscription. When new messages arrive for this
     *        subscription, the {@link MessageHandler#onMessage(Message)} method will be invoked.
     * @see MessageHandler#onMessage(Message)
     */
    void setMessageHandler(MessageHandler cb);
}
