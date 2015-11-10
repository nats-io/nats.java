/**
 * 
 */
package io.nats.client;

/**
 * @author larry
 *
 */
public interface Connection {
	public Subscription subscribe(String subject, MessageHandler cb);
	public Subscription subscribe(String subject, String queue, MessageHandler cb);

}
