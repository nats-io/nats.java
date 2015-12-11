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
			throws IOException, IllegalStateException, TimeoutException;
	public Message nextMessage(long timeout) 
			throws TimeoutException, IOException, IllegalStateException;

}
