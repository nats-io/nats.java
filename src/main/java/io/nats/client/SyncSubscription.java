/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/
/**
 * 
 */

package io.nats.client;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A client uses an {@code SyncSubscription} object to receive messages from a subject
 * synchronously.
 *
 */
public interface SyncSubscription extends Subscription {

    /**
     * Receives the next {@code Message} that becomes available for this {@code Subscription},
     * waiting if necessary until a {@code Message} becomes available.
     * 
     * @return the next message produced for this subscription, or return null if the
     *         {@code Connection} is closed concurrently. <br><br>This call blocks indefinitely
     *         until a message is produced or until this Subscription is closed.
     * @throws InterruptedException if interrupted while waiting, e.g. because the
     *         {@code Subscription} was closed in another thread
     * @throws IOException if the {@code Subscription} has been unsubscribed due to reaching its
     *         autoUnsubscribe limit, or if the {@code Subscription} has been marked a slow
     *         consumer.
     * @throws IllegalStateException if the {@code Subscription} is not valid, e.g. it was closed
     *         prior to this invocation
     * @see Subscription#autoUnsubscribe(int)
     */
    Message nextMessage() throws IOException, InterruptedException;

    /**
     * Receives the next {@code Message} that arrives for this {@code Subscription}, waiting up to
     * the specified wait time if necessary for a {@code Message} to become available.
     * 
     * @param timeout how long to wait before giving up, in milliseconds
     * @return the next message produced for this subscription, or null if timeout expires before a
     *         message is available
     * @throws TimeoutException if the timeout expires before a {@code Message} becomes available
     * @throws InterruptedException if interrupted while waiting, e.g. because the
     *         {@code Subscription} was closed in another thread
     * @throws IOException if the {@code Subscription} has been unsubscribed due to reaching its
     *         autoUnsubscribe limit, or if the {@code Subscription} has been marked a slow
     *         consumer.
     * @throws IllegalStateException if the {@code Subscription} is not valid, e.g. it was closed
     *         prior to this invocation
     * @see #nextMessage(long, TimeUnit)
     * @see Subscription#autoUnsubscribe(int)
     */
    Message nextMessage(long timeout) throws IOException, TimeoutException, InterruptedException;

    /**
     * Receives the next {@code Message} that arrives for this {@code Subscription}, waiting up to
     * the specified wait time if necessary for a {@code Message} to become available.
     * 
     * @param timeout how long to wait before giving up, in units of {@code unit}
     * @param unit a {@code TimeUnit} determining how to interpret the timeout parameter
     * @return the next message produced for this subscription, or return null if timeout expires
     * @throws TimeoutException if the timeout expires before a {@code Message} becomes available
     * @throws InterruptedException if interrupted while waiting, e.g. because the
     *         {@code Subscription} was closed in another thread
     * @throws IOException if the {@code Subscription} has been unsubscribed due to reaching its
     *         autoUnsubscribe limit, or if the {@code Subscription} has been marked a slow
     *         consumer.
     * @throws IllegalStateException if the {@code Subscription} is not valid, e.g. it was closed
     *         prior to this invocation
     * @see Subscription#autoUnsubscribe(int)
     */
    Message nextMessage(long timeout, TimeUnit unit)
            throws IOException, TimeoutException, InterruptedException;
}
