/**
 * 
 */
package io.nats.client;

/**
 * @author larry
 *
 */
public interface SyncSubscription extends Subscription {
	public Message nextMessage() throws BadSubscriptionException, ConnectionClosedException, SlowConsumerException, MaxMessagesException, TimeoutException;
	public Message nextMessage(long timeout) 
			throws BadSubscriptionException, ConnectionClosedException, SlowConsumerException, MaxMessagesException, TimeoutException;

}
