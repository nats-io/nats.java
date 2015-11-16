/**
 * 
 */
package io.nats.client;

import java.io.IOException;

public interface Connection {
	public AsyncSubscription subscribe(String subject, MessageHandler cb);
	public AsyncSubscription subscribeAsync(String subject, MessageHandler cb);
	public Subscription subscribe(String subject, String queue, MessageHandler cb);
	public SyncSubscription subscribeSync(String subj);
	public SyncSubscription subscribeSync(String subject, String queue);
	public Subscription QueueSubscribe(String subject, String queue, MessageHandler cb);
	public SyncSubscription QueueSubscribeSync(String subject, String queue);
	
	public void publish(String subject, byte[] data) throws ConnectionClosedException;
	public void publish(Message msg) throws ConnectionClosedException;
	public void publish(String subject, String reply, byte[] data) throws ConnectionClosedException;
	
    public Message request(String subject, byte[] data, long timeout) 
    		throws ConnectionClosedException, BadSubscriptionException, SlowConsumerException, MaxMessagesException, IOException;
    public Message request(String subject, byte[] data) throws ConnectionClosedException, BadSubscriptionException, SlowConsumerException, MaxMessagesException, IOException;
    
    public String newInbox();
    
	public void close();

    boolean isClosed();
    boolean isReconnecting();
    
    public Statistics getStats();
    public void resetStats();
    
    public long getMaxPayload();
	void flush(int timeout) throws Exception;
	void flush() throws Exception;
	void setDisconnectedEventHandler(ConnEventHandler disconnectedEventHandler);
	ConnEventHandler getReconnectedEventHandler();
	void setReconnectedEventHandler(ConnEventHandler reconnectedEventHandler);
	ConnExceptionHandler getExceptionHandler();
	void setExceptionHandler(ConnExceptionHandler exceptionHandler);
	ConnEventHandler getClosedEventHandler();
	void setClosedEventHandler(ConnEventHandler closedEventHandler);
	ConnEventHandler getDisconnectedEventHandler();
    
    

}
