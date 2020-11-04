// Copyright 2015-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.client;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

/**
 * The Connection class is at the heart of the NATS Java client. Fundamentally a connection represents
 * a single network connection to the NATS server.
 * 
 * <p>Each connection you create will result in the creation of a single socket and several threads:
 * <ul>
 * <li> A reader thread for taking data off the socket
 * <li> A writer thread for putting data onto the socket
 * <li> A timer thread for a few maintenance timers
 * <li> A dispatch thread to handle request/reply traffic
 * </ul>
 * 
 * <p>The connection has a {@link Connection.Status status} which can be checked using the {@link #getStatus() getStatus}
 * method or watched using a {@link ConnectionListener ConnectionListener}.
 * 
 * <p>Connections, by default, are configured to try to reconnect to the server if there is a network failure up to
 * {@link Options#DEFAULT_MAX_RECONNECT times}. You can configure this behavior in the {@link Options Options}.
 * Moreover, the options allows you to control whether reconnect happens in the same order every time, and the time
 * to wait if trying to reconnect to the same server over and over.
 * 
 * <p>The list of servers used for connecting is provided by the {@link Options Options}. The list of servers used
 * during reconnect can be an expanded list. This expansion comes from the connections most recent server. For example,
 * if you connect to serverA, it can tell the connection &quot;i know about serverB and serverC&quot;. If serverA goes down
 * the client library will try to connect to serverA, serverB and serverC. Now, if the library connects to serverB, it may tell the client 
 * &quot;i know about serverB and serverE&quot;. The client's list of servers, available from {@link #getServers() getServers()}
 * will now be serverA from the initial connect, serverB and serverE, the reference to serverC is lost.
 * 
 * <p>When a connection is {@link #close() closed} the thread and socket resources are cleaned up.
 * 
 * <p>All outgoing messages are sent through the connection object using one of the two 
 * {@link #publish(String, byte[]) publish} methods or the {@link #request(String, byte[]) request} method.
 * When publishing you can specify a reply to subject which can be retrieved by the receiver to respond.
 * The request method will handle this behavior itself, but it relies on getting the value out of a Future
 * so may be less flexible than publish with replyTo set.
 * 
 * <p>Messages can be received in two ways. You can create a Subscription which will allow you to read messages
 * synchronously using the {@link Subscription#nextMessage(Duration) nextMessage} method or you can create a 
 * {@link Dispatcher Dispatcher}. The Dispatcher will create a thread to listen for messages on one or more subscriptions.
 * The Dispatcher groups a set of subscriptions into a single listener thread that calls application code
 * for each messages.
 * 
 * <p>Applications can use the {@link #flush(Duration) flush} method to check that published messages have 
 * made it to the server. However, this method initiates a round trip to the server and waits for the response so
 * it should be used sparingly.
 * 
 * <p>The connection provides two listeners via the Options. The {@link ConnectionListener ConnectionListener}
 * can be used to listen for lifecycle events. This listener is required for
 *  {@link Nats#connectAsynchronously(Options, boolean) connectAsynchronously}, but otherwise optional. The
 * {@link ErrorListener ErrorListener} provides three callback opportunities including slow consumers, error
 * messages from the server and exceptions handled by the client library. These listeners can only be set at creation time
 * using the {@link Options options}.
 * 
 * <p><em>Note</em>: The publish methods take an array of bytes. These arrays <strong>will not be copied</strong>. This design choice
 * is based on the common case of strings or objects being converted to bytes. Once a client can be sure a message was received by
 * the NATS server it is theoretically possible to reuse that byte array, but this pattern should be treated as advanced and only used
 * after thorough testing. 
 */
public interface Connection extends AutoCloseable {

    public enum Status {
        /**
         * The {@code Connection} is not connected.
         */
        DISCONNECTED,
        /**
         * The {@code Connection} is currently connected.
         */
        CONNECTED,
        /**
         * The {@code Connection} is currently closed.
         */
        CLOSED,
        /**
         * The {@code Connection} is currently attempting to reconnect to a server from its server list.
         */
        RECONNECTING,
        /**
         * The {@code Connection} is currently connecting to a server for the first
         * time.
         */
        CONNECTING;
    }

    /**
     * Send a message to the specified subject. The message body <strong>will
     * not</strong> be copied. The expected usage with string content is something
     * like:
     * 
     * <pre>
     * nc = Nats.connect()
     * nc.publish("destination", "message".getBytes("UTF-8"))
     * </pre>
     * 
     * where the sender creates a byte array immediately before calling publish.
     * 
     * See {@link #publish(String, String, byte[]) publish()} for more details on 
     * publish during reconnect.
     * 
     * @param subject the subject to send the message to
     * @param body the message body
     * @throws IllegalStateException if the reconnect buffer is exceeded
     */
    public void publish(String subject, byte[] body);

    /**
     * Send a request to the specified subject, providing a replyTo subject. The
     * message body <strong>will not</strong> be copied. The expected usage with
     * string content is something like:
     * 
     * <pre>
     * nc = Nats.connect()
     * nc.publish("destination", "reply-to", "message".getBytes("UTF-8"))
     * </pre>
     * 
     * where the sender creates a byte array immediately before calling publish.
     * <p>
     * During reconnect the client will try to buffer messages. The buffer size is set
     * in the connect options, see {@link Options.Builder#reconnectBufferSize(long) reconnectBufferSize()}
     * with a default value of {@link Options#DEFAULT_RECONNECT_BUF_SIZE 8 * 1024 * 1024} bytes.
     * If the buffer is exceeded an IllegalStateException is thrown. Applications should use
     * this exception as a signal to wait for reconnect before continuing.
     * </p>
     * @param subject the subject to send the message to
     * @param replyTo the subject the receiver should send the response to
     * @param body the message body
     * @throws IllegalStateException if the reconnect buffer is exceeded
     */
    public void publish(String subject, String replyTo, byte[] body);

    /**
     * Send a request. The returned future will be completed when the
     * response comes back.
     * 
     * @param subject the subject for the service that will handle the request
     * @param data the content of the message
     * @return a Future for the response, which may be cancelled on error or timed out
     */
    public CompletableFuture<Message> request(String subject, byte[] data);

    /**
     * Send a request and returns the reply or null. This version of request is equivalent
     * to calling get on the future returned from {@link #request(String, byte[]) request()} with
     * the timeout and handling the ExecutionException and TimeoutException.
     * 
     * @param subject the subject for the service that will handle the request
     * @param data the content of the message
     * @param timeout the time to wait for a response
     * @return the reply message or null if the timeout is reached
     * @throws InterruptedException if one is thrown while waiting, in order to propogate it up
     */
    public Message request(String subject, byte[] data, Duration timeout) throws InterruptedException;

    /**
     * Create a synchronous subscription to the specified subject.
     * 
     * <p>Use the {@link io.nats.client.Subscription#nextMessage(Duration) nextMessage}
     * method to read messages for this subscription.
     * 
     * <p>See {@link #createDispatcher(MessageHandler) createDispatcher} for
     * information about creating an asynchronous subscription with callbacks.
     * 
     * <p>As of 2.6.1 this method will throw an IllegalArgumentException if the subject contains whitespace.
     * 
     * @param subject the subject to subscribe to
     * @return an object representing the subscription
     */
    public Subscription subscribe(String subject);

    /**
     * Create a synchronous subscription to the specified subject and queue.
     * 
     * <p>Use the {@link Subscription#nextMessage(Duration) nextMessage} method to read
     * messages for this subscription.
     * 
     * <p>See {@link #createDispatcher(MessageHandler) createDispatcher} for
     * information about creating an asynchronous subscription with callbacks.
     * 
     * <p>As of 2.6.1 this method will throw an IllegalArgumentException if either string contains whitespace.
     * 
     * @param subject the subject to subscribe to
     * @param queueName the queue group to join
     * @return an object representing the subscription
     */
    public Subscription subscribe(String subject, String queueName);

    /**
     * Create a {@code Dispatcher} for this connection. The dispatcher can group one
     * or more subscriptions into a single callback thread. All messages go to the
     * same {@code MessageHandler}.
     * 
     * <p>Use the Dispatcher's {@link Dispatcher#subscribe(String)} and
     * {@link Dispatcher#subscribe(String, String)} methods to add subscriptions.
     * 
     * <pre>
     * nc = Nats.connect()
     * d = nc.createDispatcher((m) -&gt; System.out.println(m)).subscribe("hello");
     * </pre>
     * 
     * @param handler The target for the messages
     * @return a new Dispatcher
     */
    public Dispatcher createDispatcher(MessageHandler handler);

    /**
     * Close a dispatcher. This will unsubscribe any subscriptions and stop the delivery thread.
     * 
     * <p>Once closed the dispatcher will throw an exception on subsequent subscribe or unsubscribe calls.
     * 
     * @param dispatcher the dispatcher to close
     */
    public void closeDispatcher(Dispatcher dispatcher);

    /**
     * Flush the connection's buffer of outgoing messages, including sending a
     * protocol message to and from the server. Passing null is equivalent to
     * passing 0, which will wait forever.
     * 
     * If called while the connection is closed, this method will immediately
     * throw a TimeoutException, regardless of the timeout.
     * 
     * If called while the connection is disconnected due to network issues this
     * method will wait for up to the timeout for a reconnect or close.
     * 
     * @param timeout The time to wait for the flush to succeed, pass 0 to wait
     *                    forever.
     * @throws TimeoutException if the timeout is exceeded
     * @throws InterruptedException if the underlying thread is interrupted
     */
    public void flush(Duration timeout) throws TimeoutException, InterruptedException;

    /**
     * Drain tells the connection to process in flight messages before closing.
     * 
     * Drain initially drains all of the consumers, stopping incoming messages.
     * Next, publishing is halted and a flush call is used to insure all published
     * messages have reached the server.
     * Finally the connection is closed.
     * 
     * In order to drain subscribers, an unsub protocol message is sent to the server followed by a flush.
     * These two steps occur before drain returns. The remaining steps occur in a background thread.
     * This method tries to manage the timeout properly, so that if the timeout is 1 second, and the flush
     * takes 100ms, the remaining steps have 900ms in the background thread.
     * 
     * The connection will try to let all messages be drained, but when the timeout is reached
     * the connection is closed and any outstanding dispatcher threads are interrupted.
     * 
     * A future is used to allow this call to be treated as synchronous or asynchronous as
     * needed by the application. The value of the future will be true if all of the subscriptions
     * were drained in the timeout, and false otherwise. The future is completed after the connection
     * is closed, so any connection handler notifications will happen before the future completes.
     * 
     * @param timeout The time to wait for the drain to succeed, pass 0 to wait
     *                    forever. Drain involves moving messages to and from the server
     *                    so a very short timeout is not recommended. If the timeout is reached before
     *                    the drain completes, the connection is simply closed, which can result in message
     *                    loss.
     * @return A future that can be used to check if the drain has completed
     * @throws InterruptedException if the thread is interrupted
     * @throws TimeoutException if the initial flush times out
     */
    public CompletableFuture<Boolean> drain(Duration timeout) throws TimeoutException, InterruptedException;

    /**
     * Close the connection and release all blocking calls like {@link #flush flush}
     * and {@link Subscription#nextMessage(Duration) nextMessage}.
     * 
     * If close() is called after {@link #drain(Duration) drain} it will wait up to the connection timeout
     * to return, but it will not initiate a close. The drain takes precedence and will initiate the close.
     * 
     * @throws InterruptedException if the thread, or one owned by the connection is interrupted during the close
     */
    public void close() throws InterruptedException ;

    /**
     * Returns the connections current status.
     * 
     * @return the connection's status
     */
    public Status getStatus();

    /**
     * MaxPayload returns the size limit that a message payload can have. This is
     * set by the server configuration and delivered to the client upon connect.
     * 
     * @return the maximum size of a message payload
     */
    public long getMaxPayload();

    /**
     * Return the list of known server urls, including additional servers discovered
     * after a connection has been established.
     * 
     * @return this connection's list of known server URLs
     */
    public Collection<String> getServers();

    /**
     * @return a wrapper for useful statistics about the connection
     */
    public Statistics getStatistics();

    /**
     * @return the read-only options used to create this connection
     */
    public Options getOptions();

    /**
     * @return the url used for the current connection, or null if disconnected
     */
    public String getConnectedUrl();
    
    /**
     * @return the error text from the last error sent by the server to this client
     */
    public String getLastError();

    /**
     * @return a new inbox subject, can be used for directed replies from
     * subscribers. These are guaranteed to be unique, but can be shared and subscribed
     * to by others.
     */
    public String createInbox();
}
