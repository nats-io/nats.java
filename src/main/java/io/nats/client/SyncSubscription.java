/**
 * 
 */
package io.nats.client;

/**
 * @author larry
 *
 */
public interface SyncSubscription extends Subscription {
	public Message nextMessage() throws BadSubscriptionException, ConnectionClosedException, SlowConsumerException, MaxMessagesException;
	public Message nextMessage(long timeout) 
			throws BadSubscriptionException, ConnectionClosedException, SlowConsumerException, MaxMessagesException;

}
