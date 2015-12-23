package io.nats.client;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import io.nats.client.ConnectionImpl.ServerInfo;

import static io.nats.client.Constants.*;

interface AbstractConnection extends AutoCloseable {
    public String newInbox();
    
	public void close();
    
	boolean isClosed();
    
	boolean isReconnecting();
    
    public Statistics getStats();
    
    public void resetStats();
    
    public long getMaxPayload();
	
    void flush(int timeout) throws IOException, TimeoutException, IllegalStateException, Exception;
	
    void flush() throws IOException, TimeoutException, IllegalStateException, Exception;
	    
	ExceptionHandler getExceptionHandler();
	void setExceptionHandler(ExceptionHandler exceptionHandler);
	
	ClosedEventHandler getClosedEventHandler();
	void setClosedEventHandler(ClosedEventHandler closedEventHandler);
	
	DisconnectedEventHandler getDisconnectedEventHandler();
	void setDisconnectedEventHandler(DisconnectedEventHandler disconnectedEventHandler);

    ReconnectedEventHandler getReconnectedEventHandler();
	void setReconnectedEventHandler(ReconnectedEventHandler reconnectedEventHandler);

	public String getConnectedUrl();

	public String getConnectedId();

	ConnState getState();

	ServerInfo getConnectedServerInfo();
}
