/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import java.io.IOException;

/**
 * A client uses a {@code Subscription} object to receive messages that have been published to a
 * subject.
 * <p>
 * <p>Each {@code Subscription} object is unique, even if the subscription is to the same subject.
 * This means that if {@code Connection.subscribe("foo", cb)} is called twice in a row, each of the
 * resulting {@code Subscription} objects will be unique, and any message delivered on subject "foo"
 * will be delivered individually to both {@code Subscription} objects.
 */
public interface Subscription extends AutoCloseable {

    /**
     * Retrieves the subject of interest from the {@code Subscription} object.
     *
     * @return the subject of interest
     */
    String getSubject();

    /**
     * Returns the optional queue group name. If present, all subscriptions with the same name will
     * form a distributed queue, and each message will only be processed by one member of the group.
     *
     * @return the name of the queue group this Subscription belongs to.
     */
    String getQueue();

    /**
     * Returns whether the Subscription object is still active (subscribed).
     *
     * @return true if the Subscription is active, false otherwise.
     */
    boolean isValid();

    /**
     * Removes interest in the {@code Subscription} object's subject immediately.
     *
     * @throws IOException if an error occurs while notifying the server
     */
    void unsubscribe() throws IOException;

    /**
     * Issues an automatic unsubscribe request. The unsubscribe is executed by the server when max
     * messages have been received. This can be useful when sending a request to an unknown number
     * of subscribers. request() uses this functionality.
     *
     * @param max The number of messages to receive before unsubscribing.
     * @throws IOException if an error occurs while sending the unsubscribe request to the NATS
     *                     server.
     */
    void autoUnsubscribe(int max) throws IOException;

    /**
     * Returns the number of messages delivered to, but not processed, by this Subscription.
     *
     * @return the number of delivered messages.
     */
    long getDelivered();

    /**
     * Returns the number of messages delivered to, but not processed, by this Subscription.
     *
     * @return the number of delivered messages.
     * @deprecated use getPending instead
     */
    @Deprecated
    int getQueuedMessageCount();

    /**
     * Returns the current number of pending messages for this subscription.
     *
     * @return the current number of pending messages for this subscription
     */
    int getPendingMsgs();

    /**
     * Returns the current number of pending bytes for this subscription.
     *
     * @return the current number of pending bytes for this subscription
     */
    int getPendingBytes();

    /**
     * Returns the maximum number of pending messages seen for this subscription.
     *
     * @return the maximum number of pending messages seen for this subscription
     */
    int getPendingMsgsMax();

    /**
     * Returns the maximum number of pending bytes seen for this subscription.
     *
     * @return the maximum number of pending bytes seen for this subscription
     */
    long getPendingBytesMax();

    /**
     * Sets the maximum number of unprocessed messages and bytes that can be left pending on this
     * subscription before messages are dropped and a slow consumer exception is thrown.
     *
     * @param msgs  the maximum number of pending messages
     * @param bytes the maximum number of pending bytes
     */
    void setPendingLimits(int msgs, int bytes);

    /**
     * Returns the maximum number of unprocessed messages that can be left pending on this
     * subscription before messages are dropped and a slow consumer exception is thrown.
     *
     * @return the maximum number of unprocessed messages that can be left pending on this
     * subscription before messages are dropped and a slow consumer exception is thrown.
     */
    int getPendingMsgsLimit();

    /**
     * Returns the maximum number of unprocessed message bytes that can be left pending on this
     * subscription before messages are dropped and a slow consumer exception is thrown.
     *
     * @return the maximum number of unprocessed message bytes that can be left pending on this
     * subscription before messages are dropped and a slow consumer exception is thrown.
     */
    int getPendingBytesLimit();

    /**
     * Clears the the maximum number of unprocessed messages and bytes for this subscription.
     */
    void clearMaxPending();

    /**
     * Returns the number of messages that this subscription has dropped due to "slow consumer"
     * condition.
     *
     * @return the number of messages that have been dropped by this Subscription
     */
    int getDropped();

    /**
     * {@inheritDoc}.
     */
    @Override
    void close();


}
