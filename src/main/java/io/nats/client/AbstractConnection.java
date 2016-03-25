/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import io.nats.client.Constants.ConnState;

/**
 * AbstractConnection is the base interface for all Connection variants.
 */
interface AbstractConnection extends AutoCloseable {

    /**
     * Creates a {@code AsyncSubscription} with interest in a given subject, assign the callback,
     * and immediately start receiving messages
     * 
     * @param subject the subject of interest
     * @param cb a {@code MessageHandler} object used to process messages received by the
     *        {@code AsyncSubscription}
     * @return the started {@code AsyncSubscription}
     * @throws IllegalArgumentException if the subject (or queue) name contains illegal characters.
     * @throws NullPointerException if the subject name is null
     * @throws IllegalStateException if the connection is closed
     */
    public AsyncSubscription subscribe(String subject, MessageHandler cb);

    /**
     * Creates an asynchronous queue subscriber on a given subject of interest. All subscribers with
     * the same queue name will form the queue group and only one member of the group will be
     * selected to receive any given message asynchronously.
     * 
     * @param subject the subject of interest
     * @param queue the name of the queue group
     * @param cb a {@code MessageHandler} object used to process messages received by the
     *        {@code Subscription}
     * @return {@code Subscription}
     */
    public Subscription subscribe(String subject, String queue, MessageHandler cb);

    /**
     * Creates a {@code AsyncSubscription} with interest in a given subject. In order to receive
     * messages, a {@code MessageHandler} must be registered, and {@link AsyncSubscription#start()}
     * must be called.
     * 
     * @param subject the subject of interest
     * @return the {@code AsyncSubscription}
     * @throws IllegalArgumentException if the subject name contains illegal characters.
     * @throws NullPointerException if the subject name is null
     * @throws IllegalStateException if the connection is closed
     */
    public AsyncSubscription subscribeAsync(String subject);

    /**
     * Creates a {@code AsyncSubscription} with interest in a given subject, assign the callback,
     * and immediately start receiving messages
     *
     * @param subject the subject of interest
     * @param cb a {@code MessageHandler} object used to process messages received by the
     *        {@code AsyncSubscription}
     * @return the started {@code AsyncSubscription}
     * @throws IllegalArgumentException if the subject (or queue) name contains illegal characters.
     * @throws NullPointerException if the subject name is null
     * @throws IllegalStateException if the connection is closed
     */
    public AsyncSubscription subscribeAsync(String subject, MessageHandler cb);

    /**
     * Creates an asynchronous queue subscriber on a given subject of interest. All subscribers with
     * the same queue name will form the queue group and only one member of the group will be
     * selected to receive any given message. In order to receive messages, a {@code MessageHandler}
     * must be registered, and {@link AsyncSubscription#start()} must be called.
     *
     * @param subject the subject of interest
     * @param queue the name of the queue group
     * @return the {@code Subscription}
     * @throws IllegalArgumentException if the subject (or queue) name contains illegal characters.
     * @throws NullPointerException if the subject name is null
     * @throws IllegalStateException if the connection is closed
     */
    public AsyncSubscription subscribeAsync(String subject, String queue);

    /**
     * Create an {@code AsyncSubscription} with interest in a given subject, assign the message
     * callback, and immediately start receiving messages.
     * 
     * @param subject the subject of interest
     * @param queue the name of the queue group
     * @param cb a message callback for this subscription
     * @return the {@code AsyncSubscription}
     * @throws IllegalArgumentException if the subject (or queue) name contains illegal characters.
     * @throws NullPointerException if the subject name is null
     * @throws IllegalStateException if the connection is closed
     */
    public AsyncSubscription subscribeAsync(String subject, String queue, MessageHandler cb);

    /**
     * Creates a synchronous queue subscriber on a given subject of interest. All subscribers with
     * the same queue name will form the queue group and only one member of the group will be
     * selected to receive any given message. {@code MessageHandler} must be registered, and
     * {@link AsyncSubscription#start()} must be called.
     *
     * @param subject the subject of interest
     * @param queue the queue group
     * @return the {@code SyncSubscription}
     * @throws IllegalArgumentException if the subject (or queue) name contains illegal characters.
     * @throws NullPointerException if the subject name is null
     * @throws IllegalStateException if the connection is closed
     */
    public SyncSubscription subscribeSync(String subject, String queue);

    /**
     * Creates a {@code AsyncSubscription} with interest in a given subject. In order to receive
     * messages, a {@code MessageHandler} must be registered, and {@link AsyncSubscription#start()}
     * must be called.
     * 
     * @param subject the subject of interest
     * @return the {@code AsyncSubscription}
     * @throws IllegalArgumentException if the subject name contains illegal characters.
     * @throws NullPointerException if the subject name is null
     * @throws IllegalStateException if the connection is closed
     */
    public SyncSubscription subscribeSync(String subject);

    /**
     * Creates a new, uniquely named inbox with the prefix '_INBOX.'
     * 
     * @return the newly created inbox subject
     */
    String newInbox();

    /**
     * Closes the connection, also closing all subscriptions on this connection.
     * 
     * <p>
     * When {@code close()} is called, the following things happen, in order:
     * <ol>
     * <li>The Connection is flushed, and any other pending flushes previously requested by the user
     * are immediately cleared.
     * <li>Message delivery to all active subscriptions is terminated immediately, without regard to
     * any messages that may have been delivered to the client's connection, but not to the relevant
     * subscription(s). Any such undelivered messages are discarded immediately.
     * <li>The DisconnectedCallback, if registered, is invoked.
     * <li>The ClosedCallback, if registered, is invoked.
     * <li>The TCP/IP socket connection to the NATS server is gracefully closed.
     * </ol>
     * 
     * @see java.lang.AutoCloseable#close()
     */
    void close();

    /**
     * Indicates whether the connection has been closed.
     * 
     * @return true if the connection is closed.
     */
    boolean isClosed();

    /**
     * Indicates whether the connection is currently reconnecting.
     * 
     * @return whether or not the connection is currently reconnecting
     */
    boolean isReconnecting();

    /**
     * Retrieves the connection statistics.
     * 
     * @return the statistics for this connection.
     * @see Statistics
     */
    Statistics getStats();

    /**
     * Resets the gathered statistics for this connection.
     * 
     * @see Statistics
     */
    void resetStats();

    /**
     * Gets the maximum payload size this connection will accept.
     * 
     * @return the maximum message payload size in bytes.
     */
    long getMaxPayload();

    /**
     * Flushes the current connection, waiting up to {@code timeout} for successful completion.
     * 
     * @param timeout - the connection timeout in milliseconds.
     * @throws IOException if a connection-related error prevents the flush from completing
     * @throws TimeoutException if the connection does not complete within the specified interval
     * @throws Exception if some other error occurs
     */
    void flush(int timeout) throws IOException, TimeoutException, Exception;

    /**
     * Flushes the current connection, waiting up to 60 seconds for completion.
     * 
     * @throws IOException if a connection-related issue prevented the flush from completing
     *         successfully
     * @throws Exception if some other error is encountered
     * @see #flush(int)
     */
    void flush() throws IOException, Exception;

    /**
     * Returns the connection's asynchronous exception callback.
     * 
     * @return the asynchronous exception handler for this connection
     * @see ExceptionHandler
     */
    ExceptionHandler getExceptionHandler();

    /**
     * Sets the connection's asynchronous exception callback.
     * 
     * @param exceptionHandler the asynchronous exception handler to set for this connection
     * @see ExceptionHandler
     */
    void setExceptionHandler(ExceptionHandler exceptionHandler);

    /**
     * Returns the connection closed callback.
     * 
     * @return the connection closed callback for this connection
     * @see ClosedCallback
     */
    ClosedCallback getClosedCallback();

    /**
     * Sets the connection closed callback.
     * 
     * @param cb the connection closed callback to set
     */
    void setClosedCallback(ClosedCallback cb);

    /**
     * Returns the connection disconnected callback.
     * 
     * @return the disconnected callback for this conection
     */
    DisconnectedCallback getDisconnectedCallback();

    /**
     * Sets the connection disconnected callback.
     * 
     * @param cb the disconnected callback to set
     */
    void setDisconnectedCallback(DisconnectedCallback cb);

    /**
     * Returns the connection reconnected callback.
     * 
     * @return the reconnect callback for this connection
     */
    ReconnectedCallback getReconnectedCallback();

    /**
     * Sets the connection reconnected callback.
     * 
     * @param cb the reconnect callback to set for this connection
     */
    void setReconnectedCallback(ReconnectedCallback cb);

    /**
     * Returns the URL string of the currently connected NATS server.
     * 
     * @return the URL string of the currently connected NATS server.
     */
    String getConnectedUrl();

    /**
     * Returns the unique server ID string of the connected server.
     * 
     * @return the unique server ID string of the connected server
     */
    String getConnectedServerId();

    /**
     * Returns the current connection state.
     * 
     * @return the current connection state
     * @see Constants.ConnState
     */
    ConnState getState();

    /**
     * Returns the details from the INFO protocol message received.
     * 
     * @return the details from the INFO protocol message received from the NATS server on initial
     *         establishment of a TCP connection.
     * @see ServerInfo
     */
    ServerInfo getConnectedServerInfo();

    /**
     * Returns the last exception registered on the connection.
     * 
     * @return the last exception registered on this connection
     */
    Exception getLastException();

    /**
     * Returns the number of valid bytes in the pending output buffer. This buffer is only used
     * during disconnect/reconnect sequences to buffer messages that are published during a
     * temporary disconnection.
     * 
     * @return the number of valid bytes in the pending output buffer.
     */
    int getPendingByteCount();
}
