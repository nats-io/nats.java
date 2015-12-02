/**
 * 
 */
package io.nats.client;

import java.util.concurrent.TimeoutException;

/**
 * @author larry
 *
 */
public interface SyncSubscription extends Subscription {
	public Message nextMessage() throws BadSubscriptionException, ConnectionClosedException, SlowConsumerException, MaxMessagesException, TimeoutException;
	public Message nextMessage(long timeout) 
			throws BadSubscriptionException, ConnectionClosedException, SlowConsumerException, MaxMessagesException, TimeoutException;

}
