/**
 * 
 */
package io.nats.client;

/**
 * @author larry
 *
 */
public interface SyncSubscription extends Subscription {
	public Message nextMsg() throws BadSubscriptionException, ConnectionClosedException, SlowConsumerException, MaxMessagesException;
	public Message nextMsg(long timeout) 
			throws BadSubscriptionException, ConnectionClosedException, SlowConsumerException, MaxMessagesException;

}
