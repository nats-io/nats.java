package io.nats.client;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import static io.nats.client.Constants.*;

/**
 *
 */
interface AbstractConnection extends AutoCloseable {

	/**
	 * Creates a new, uniquely named inbox with the prefix '_INBOX.'
	 * @return the newly created inbox subject
	 */
	String newInbox();
    
	/* (non-Javadoc)
	 * @see java.lang.AutoCloseable#close()
	 */
	void close();
    
	/**
	 * @return whether or not the connection is closed.
	 */
	boolean isClosed();
    
	/**
	 * @return whether or not the connection is currently reconnecting
	 * (ConnectionImpl.class#ConnState.RECONNECTING)
	 */
	boolean isReconnecting();
    
    /**
     * @return the statistics for this connection.
     * @see Statistics
     */
    Statistics getStats();
    
    /**
     * Resets the gathered statistics for this connection.
     *  @see Statistics
     */
    void resetStats();
    
    /**
     * @return the maximum message payload size for this connection.
     */
    long getMaxPayload();
	
    /**
     * @param timeout - the connection timeout in milliseconds.
     * @throws IOException - if a connection-related error prevents
     * the flush from completing
     * @throws TimeoutException - if the connection does not complete
     *                            within the specified interval
     * @throws Exception - if some other error occurs
     */
    void flush(int timeout) throws IOException, TimeoutException, Exception;
	
    /**
     * A blocking connection flush.
     * @see #flush(int)
     * @throws IOException if a connection-related error prevents
     * the flush from completing
     * @throws TimeoutException if the flush operation doesn't complete before 
     * {@code timeout} expires
     * @throws Exception if some other downstream exception causes the flush to fail
     */
    void flush() throws IOException, TimeoutException, Exception;
	    
	/**
	 * @return the asynchronous exception handler for this connection
	 * @see ExceptionHandler
	 */
	ExceptionHandler getExceptionHandler();
	/**
	 * @param exceptionHandler the asynchronous exception handler to set for 
	 * this connection
	 * @see ExceptionHandler
	 */
	void setExceptionHandler(ExceptionHandler exceptionHandler);
	
	/**
	 * @return the connection closed callback for this connection
	 * @see ClosedCallback
	 */
	ClosedCallback getClosedCallback();
	/**
	 * @param cb the connection closed callback to set
	 */
	void setClosedCallback(ClosedCallback cb);
	
	/**
	 * @return the disconnected callback for this conection
	 */
	DisconnectedCallback getDisconnectedCallback();
	/**
	 * @param cb the disconnected callback to set
	 */
	void setDisconnectedCallback(DisconnectedCallback cb);

    /**
     * @return the reconnect callback for this connection
     */
    ReconnectedCallback getReconnectedCallback();
	/**
	 * @param cb the reconnect callback to set for this connection
	 */
	void setReconnectedCallback(ReconnectedCallback cb);

	/**
	 * @return the URL string of the currently connected NATS server.
	 */
	String getConnectedUrl();

	/**
	 * @return the information from the server's INFO message
	 */
	String getConnectedServerId();

	/**
	 * @return the current connection state. 
	 * @see Constants.ConnState
	 */
	ConnState getState();

	/**
	 * @return the details from the INFO protocol message received 
	 * from the NATS server on initial establishment of a TCP connection.
	 * @see ServerInfo
	 */
	ServerInfo getConnectedServerInfo();
	
	/**
	 * @return the last error registered on this connection
	 */
	Exception getLastError();
}
