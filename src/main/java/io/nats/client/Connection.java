/**
 * 
 */
package io.nats.client;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public interface Connection extends AbstractConnection {
	public Subscription subscribe(String subject, String queue, MessageHandler cb);
	public AsyncSubscription subscribe(String subject, MessageHandler cb);
	public AsyncSubscription subscribeAsync(String subject, String string);
	public AsyncSubscription subscribeAsync(String subject, MessageHandler cb);
	public AsyncSubscription subscribeAsync(String subject, String reply, MessageHandler cb);
	public AsyncSubscription subscribeAsync(String subject);
	public SyncSubscription subscribeSync(String subj);
	public SyncSubscription subscribeSync(String subject, String queue);
	public Subscription queueSubscribe(String subject, String queue, MessageHandler cb);
	public SyncSubscription queueSubscribeSync(String subject, String queue);
	
	public void publish(String subject, byte[] data);
	public void publish(Message msg);
	public void publishRequest(String subject, String reply, byte[] data);
	
    public Message request(String subject, byte[] data, long timeout) 
    		throws TimeoutException, IOException;
    public Message request(String subject, byte[] data) 
    		throws TimeoutException, IOException;
 

}
