/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
/**
 * 
 */
package io.nats.client;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A client uses an {@code SyncSubscription} object to receive 
 * messages from a subject synchronously. 
 *
 */
public interface SyncSubscription extends Subscription {

	/**
	 * Receive the next message that becomes available for this {@code Subscription},
	 * waiting if necessary until a {@code Message} becomes available.
	 * @return the next message produced for this subscription, or return null 
	 * if the {@code Connection} is closed concurrently.
	 * @throws IOException if an I/O error prevents message delivery
	 * @throws IllegalStateException if the {@code Subscription} has been 
	 * removed (unsubscribed)
	 * @throws IOException if the {@code Subscription}'s has been 
	 * unsubscribed due to reaching its autoUnsubscribe limit.
	 * @see Subscription#autoUnsubscribe(int)
	 */
	Message nextMessage() throws IOException;

	/**
	 * Receive the next {@code Message} that arrives for this {@code Subscription} 
	 * within the specified timeout interval.
	 * @param timeout the timeout value (in milliseconds)
	 * @return the next message produced for this subscription, or return null 
	 * if timeout expires 
	 * @throws IOException if an I/O error prevents message delivery
	 * @throws TimeoutException if the timeout expires before a message becomes
	 * available
	 * @throws IllegalStateException if the {@code Subscription} has been 
	 * removed (unsubscribed)
	 * @throws IOException if the {@code Subscription}'s has been 
	 * unsubscribed due to reaching its autoUnsubscribe limit.
	 * @see #nextMessage(long, TimeUnit)
	 * @see Subscription#autoUnsubscribe(int)
	 */
	Message nextMessage(long timeout) throws IOException, TimeoutException;

	/**
	 * Receive the next {@code Message} that arrives for this {@code Subscription} 
	 * within the specified timeout interval.
	 * @param timeout how long to wait before giving up, in units of {@code unit}
	 * @param unit the timeout value
	 * @return the next message produced for this subscription, or return null 
	 * if timeout expires 
	 * @throws IOException if an I/O error prevents message delivery
	 * @throws TimeoutException if the timeout expires before a message becomes
	 * available
	 * @throws IllegalStateException if the {@code Subscription} has been 
	 * removed (unsubscribed)
	 * @throws IOException if the {@code Subscription}'s has been 
	 * unsubscribed due to reaching its autoUnsubscribe limit.
	 * @see Subscription#autoUnsubscribe(int)
	 */
	Message nextMessage(long timeout, TimeUnit unit) throws IOException, TimeoutException;
}
