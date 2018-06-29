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

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

/**
 * The Connection class is at the heart of the NATS Java client. Fundamentally a connection represents
 * a single network connection to the gnatsd server.
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
 * the gnatsd it is theoretically possible to reuse that byte array, but this pattern should be treated as advanced and only used
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
     * @param subject the subject to send the message to
     * @param body the message body
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
     * 
     * @param subject the subject to send the message to
     * @param replyTo the subject the receiver should send the response to
     * @param body the message body
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
    public Future<Message> request(String subject, byte[] data);

    /**
     * Create a synchronous subscription to the specified subject.
     * 
     * <p>Use the {@link io.nats.client.Subscription#nextMessage(Duration) nextMessage}
     * method to read messages for this subscription.
     * 
     * <p>See {@link #createDispatcher(MessageHandler) createDispatcher} for
     * information about creating an asynchronous subscription with callbacks.
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
     * @param timeout The time to wait for the flush to succeed, pass 0 to wait
     *                    forever.
     * @throws TimeoutException if the timeout is exceeded
     * @throws InterruptedException if the underlying thread is interrupted
     */
    public void flush(Duration timeout) throws TimeoutException, InterruptedException;

    /**
     * Close the connection and release all blocking calls like {@link #flush flush}
     * and {@link Subscription#nextMessage(Duration) nextMessage}.
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
}