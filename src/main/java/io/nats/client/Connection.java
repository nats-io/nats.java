/**
 * 
 */
package io.nats.client;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public interface Connection {
	public static enum ConnState {
		DISCONNECTED, CONNECTED, CLOSED, RECONNECTING, CONNECTING
	}
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
	
	public void publish(String subject, byte[] data) throws ConnectionClosedException;
	public void publish(Message msg) throws ConnectionClosedException;
	public void publish(String subject, String reply, byte[] data) throws ConnectionClosedException;
	
    public Message request(String subject, byte[] data, long timeout) 
    		throws TimeoutException, IOException;
    public Message request(String subject, byte[] data) 
    		throws TimeoutException, IOException;
    public String newInbox();
    
	public void close();

    boolean isClosed();
    boolean isReconnecting();
    
    public Statistics getStats();
    public void resetStats();
    
    public long getMaxPayload();
	void flush(int timeout) throws IOException, TimeoutException, IllegalStateException, Exception;
	void flush() throws IOException, TimeoutException, IllegalStateException, Exception;
	void setDisconnectedEventHandler(DisconnectedEventHandler disconnectedEventHandler);
	ReconnectedEventHandler getReconnectedEventHandler();
	void setReconnectedEventHandler(ReconnectedEventHandler reconnectedEventHandler);
	ExceptionHandler getExceptionHandler();
	void setExceptionHandler(ExceptionHandler exceptionHandler);
	ClosedEventHandler getClosedEventHandler();
	void setClosedEventHandler(ClosedEventHandler closedEventHandler);
	DisconnectedEventHandler getDisconnectedEventHandler();
	public String getConnectedUrl();
	ConnState getState();
    

}
