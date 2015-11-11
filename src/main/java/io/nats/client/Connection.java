/**
 * 
 */
package io.nats.client;

/**
 * @author larry
 *
 */
public interface Connection {
	Subscription subscribe(String subject, MessageHandler cb);
	Subscription subscribe(String subject, String queue, MessageHandler cb);
	Subscription subscribeSync(String subj);
	Subscription QueueSubscribe(String subject, String queue, MessageHandler cb);
	Subscription QueueSubscribeSync(String subject, String queue);
}
