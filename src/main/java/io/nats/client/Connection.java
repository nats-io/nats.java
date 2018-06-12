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
import java.util.concurrent.TimeoutException;

public interface Connection {

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
         * The {@code Connection} is currently attempting to reconnect to a server it was previously
         * connected to.
         */
        RECONNECTING,
        /**
         * The {@code Connection} is currently connecting to a server for the first time.
         */
        CONNECTING;
    }

    /**
     * Send a message to the specified subject. The message body <strong>will not</strong>
     * be copied. The expected usage with string content is something like:
     * 
     * <p><blockquote><pre>
     * nc = Nats.connect()
     * nc.publish("destination", "message".getBytes("UTF-8"))
     * </pre></blockquote></p>
     * 
     * where the sender creates a byte array immediatly before calling publish.
     * 
     * @param  subject The subject to send the message to.
     * @param  body The message body.
     */
    public void publish(String subject, byte[] body);

    /**
     * Send a request to the specified subject, providing a replyTo subject.
     * The message body <strong>will not</strong> be copied. The expected usage with string content is something like:
     * 
     * <p><blockquote><pre>
     * nc = Nats.connect()
     * nc.publish("destination", "reply-to", "message".getBytes("UTF-8"))
     * </pre></blockquote></p>
     * 
     * where the sender creates a byte array immediatly before calling publish.
     * 
     * @param  subject The subject to send the message to.
     * @param  replyTo The subject the receiver should send the response to.
     * @param  body The message body.
     */
    public void publish(String subject, String replyTo, byte[] body);

    /**
     * Create a synchronous subscription to the specified subject.
     * 
     * <p>Use the {@link io.nats.client.Subscription#nextMessage(Duration) nextMessage} method to read messages
     * for this subscription.</p>
     * 
     * <p><strong>TODO(sasbury)</strong> In the case of an error...</p>
     * 
     * <p>See {@link #createDispatcher(MessageHandler) createDispatcher} for information about creating an asynchronous
     * subscription with callbacks.</p>
     * 
     * @param  subject The subject to subscribe to.
     * @return  An object representing the subscription.
     */
    public Subscription subscribe(String subject);

    /**
     * Create a synchronous subscription to the specified subject and queue.
     * 
     * <p>Use the {@link Subscription#nextMessage(Duration) nextMessage} method to read messages
     * for this subscription.</p>
     * 
     * <p><strong>TODO(sasbury)</strong> In the case of an error...</p>
     * 
     * <p>See {@link #createDispatcher(MessageHandler) createDispatcher} for information about creating an asynchronous
     * subscription with callbacks.</p>
     * 
     * 
     * @param  subject The subject to subscribe to.
     * @param  queueName The queue group to join.
     * @return  An object representing the subscription.
     */
    public Subscription subscribe(String subject, String queueName);

    /**
     * Send a synchrounous request.
     * 
     * @param subject The subject for the service that will handle the request.
     * @param data The content of the message.
     * @param timeout The maximum time to wait for a response.
     * @return The response or null.
     */
    public Message request(String subject, byte[] data, Duration timeout);

    /**
     * Create a {@code Dispatcher} for this connection. The dispatcher can group one
     * or more subscriptions into a single callback thread. All messages go to the same {@code MessageHandler}.
     * 
     * <p>Use the Dispatcher's {@link Dispatcher#subscribe(String)} and
     * {@link Dispatcher#subscribe(String, String)} methods to add subscriptions.</p>
     * 
     * <p><blockquote><pre>
     * nc = Nats.connect()
     * d = nc.createDispatcher((m) -> System.out.println(m)).subscribe("hello");
     * </pre></blockquote></p>
     * 
     * @param handler The target for the messages.
     * @return A new Dispatcher.
     */
    public Dispatcher createDispatcher(MessageHandler handler);

    /**
     * Flush the connection's buffer of outgoing messages, including sending a protocol message
     * to and from the server.
     * 
     * @param timeout The time to wait for the flush to succeed, pass 0 to wait forever.
     */
    public void flush(Duration timeout) throws TimeoutException, InterruptedException ;

    /**
     * Close the connection and release all blocking calls like {@link #flush flush} and 
     * {@link Subscription#nextMessage(Duration) nextMessage}.
     */
    public void close();

    /**
     * Returns the connections current status.
     * 
     * @return The status.
     */
    public Status getStatus();

    /**
     * MaxPayload returns the size limit that a message payload can have.
     * This is set by the server configuration and delivered to the client upon connect.
     * 
     * @return The maximum size of a message payload.
     */
    public long getMaxPayload();

    /**
     * Return the list of known server urls, including additional servers discovered after
     *  a connection has been established.
     * 
     * @return This connections list of known server URLs.
     */
    public Collection<String> getServers();
    
    /**
     * Get some useful statistics about the client.
     */
    public Statistics getStatistics();
}