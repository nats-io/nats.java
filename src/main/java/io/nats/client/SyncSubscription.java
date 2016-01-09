/**
 * 
 */
package io.nats.client;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author larry
 *
 */
public interface SyncSubscription extends Subscription {
	public Message nextMessage() 
			throws IOException, TimeoutException;
	public Message nextMessage(long timeout) 
			throws TimeoutException, IOException;

}
