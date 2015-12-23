/**
 * 
 */
package io.nats.client;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public interface Connection extends AbstractConnection {
	public Subscription subscribe(String subject, String queue, MessageHandler cb) throws ConnectionClosedException;
	public AsyncSubscription subscribe(String subject, MessageHandler cb) throws ConnectionClosedException;
	public AsyncSubscription subscribeAsync(String subject, String string) throws ConnectionClosedException;
	public AsyncSubscription subscribeAsync(String subject, MessageHandler cb) throws ConnectionClosedException;
	public AsyncSubscription subscribeAsync(String subject, String reply, MessageHandler h) throws ConnectionClosedException;
	public AsyncSubscription subscribeAsync(String subject) throws ConnectionClosedException;
	public SyncSubscription subscribeSync(String subj) throws ConnectionClosedException;
	public SyncSubscription subscribeSync(String subject, String queue) throws ConnectionClosedException;
	public Subscription QueueSubscribe(String subject, String queue, MessageHandler cb) throws ConnectionClosedException;
	public SyncSubscription QueueSubscribeSync(String subject, String queue) throws ConnectionClosedException;
	
	public void publish(String subject, byte[] data) throws ConnectionClosedException, IllegalStateException;
	public void publish(Message msg) throws ConnectionClosedException, IllegalStateException;
	public void publish(String subject, String reply, byte[] data) throws ConnectionClosedException, IllegalStateException;
	
    public Message request(String subject, byte[] data, long timeout) 
    		throws TimeoutException, IOException;
    public Message request(String subject, byte[] data) 
    		throws TimeoutException, IOException;
 

}
