/**
 * 
 */
package io.nats.client;

/**
 * @author larry
 *
 */
public interface SyncSubscription extends Subscription {
	public Message nextMsg();
	public Message nextMsg(long timeout);

}
