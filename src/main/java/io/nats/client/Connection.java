/**
 * 
 */
package io.nats.client;

public interface Connection {
	public AsyncSubscription subscribe(String subject, MessageHandler cb);
	public AsyncSubscription subscribeAsync(String subject, MessageHandler cb);
	public AsyncSubscription subscribeAsync(String subject, String reply, MessageHandler h);
	public AsyncSubscription subscribeAsync(String subject);
	public Subscription subscribe(String subject, String queue, MessageHandler cb);
	public SyncSubscription subscribeSync(String subj);
	public SyncSubscription subscribeSync(String subject, String queue);
	public Subscription QueueSubscribe(String subject, String queue, MessageHandler cb);
	public SyncSubscription QueueSubscribeSync(String subject, String queue);
	
	public void publish(String subject, byte[] data) throws ConnectionClosedException;
	public void publish(Message msg) throws ConnectionClosedException;
	public void publish(String subject, String reply, byte[] data) throws ConnectionClosedException;
	
    public Message request(String subject, byte[] data, long timeout) 
    		throws TimeoutException, NATSException;
    public Message request(String subject, byte[] data) 
    		throws TimeoutException, NATSException;
    public String newInbox();
    
	public void close();

    boolean isClosed();
    boolean isReconnecting();
    
    public Statistics getStats();
    public void resetStats();
    
    public long getMaxPayload();
	void flush(int timeout) throws Exception;
	void flush() throws Exception;
	void setDisconnectedEventHandler(ConnectionEventHandler disconnectedEventHandler);
	ConnectionEventHandler getReconnectedEventHandler();
	void setReconnectedEventHandler(ConnectionEventHandler reconnectedEventHandler);
	ExceptionHandler getExceptionHandler();
	void setExceptionHandler(ExceptionHandler exceptionHandler);
	ConnectionEventHandler getClosedEventHandler();
	void setClosedEventHandler(ConnectionEventHandler closedEventHandler);
	ConnectionEventHandler getDisconnectedEventHandler();
	public String getConnectedUrl();
    
    

}
