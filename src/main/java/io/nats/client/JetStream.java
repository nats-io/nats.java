// Copyright 2020 The NATS Authors
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

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * JetStream context for creation and access to streams and consumers in NATS.
 */
public interface JetStream {

    /**
     * Loads or creates a stream.
     * @param stream name of the stream
     * @param config the stream configuration to use.
     */
    public void loadOrCreate(String stream, StreamConfiguration config);

    /**
     * Loads or creates a consumer.
     * @param name name of the durable
     * @param config the consumer configuration to use.
     */    
    public void loadOrCreate(String name, ConsumerConfiguration config);

    /**
     * Sets the default publish options to be used when not specified.
     * @param options the default publish options.
     */
    public void setDefaultPublishOptions(PublishOptions options);

    /**
     * Sets the default subscribe options to be used when not specified.
     * @param options the default subscribe options.
     */
    public void setDefaultSubscribeOptions(SubscribeOptions options);

    /**
     * Send a message to the specified subject and waits for a response from
     * Jetstream. The message body <strong>will not</strong> be copied. The expected
     * usage with string content is something like:
     * 
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * js.publish("destination", "message".getBytes("UTF-8"), publishOptions)
     * </pre>
     * 
     * where the sender creates a byte array immediately before calling publish.
     * 
     * See {@link #publish(String, byte[]) publish()} for more details on 
     * publish during reconnect.
     * 
     * @param subject the subject to send the message to
     * @param body the message body
     * @return The publish acknowledgement
     * @throws IllegalStateException if the reconnect buffer is exceeded
     * @throws IOException if there are communcation issues with the NATS server
     * @throws TimeoutException if the NATS server does not return a response
     * @throws InterruptedException if the thread is interrupted
     */
    public PublishAck publish(String subject, byte[] body) throws IOException, InterruptedException, TimeoutException;

    /**
     * Send a message to the specified subject and waits for a response from
     * Jetstream. The message body <strong>will not</strong> be copied. The expected
     * usage with string content is something like:
     * 
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * js.publish("destination", "message".getBytes("UTF-8"), publishOptions)
     * </pre>
     * 
     * where the sender creates a byte array immediately before calling publish.
     * 
     * See {@link #publish(String, byte[]) publish()} for more details on 
     * publish during reconnect.
     * 
     * @param subject the subject to send the message to
     * @param body the message body
     * @param options publisher options 
     * @return The publish acknowledgement
     * @throws IllegalStateException if the reconnect buffer is exceeded
     * @throws IOException if there are communcation issues with the NATS server
     * @throws TimeoutException if the NATS server does not return a response
     * @throws InterruptedException if the thread is interrupted
     */    
    public PublishAck publish(String subject, byte[] body, PublishOptions options) throws IOException, InternalError, TimeoutException, InterruptedException;

    /**
     * Create a synchronous subscription to the specified subject.
     * 
     * <p>Use the {@link io.nats.client.Subscription#nextMessage(Duration) nextMessage}
     * method to read messages for this subscription.
     * 
     * <p>See {@link io.nats.client.Connection#createDispatcher(MessageHandler) createDispatcher} for
     * information about creating an asynchronous subscription with callbacks.
     * 
     * <p>As of 2.6.1 this method will throw an IllegalArgumentException if the subject contains whitespace.
     * 
     * @param subject the subject to subscribe to
     * @param options subscription options
     * @return an object representing the subscription
     * @throws TimeoutException if the NATS server does not return a response
     * @throws InterruptedException if the thread is interrupted
     * @throws IOException if there are communcation issues with the NATS server
     */    
    public Subscription subscribe(String subject, SubscribeOptions options) throws InterruptedException, TimeoutException, IOException;

    /**
     * Create a synchronous subscription to the specified subject.
     * 
     * <p>Use the {@link io.nats.client.Subscription#nextMessage(Duration) nextMessage}
     * method to read messages for this subscription.
     * 
     * <p>See {@link io.nats.client.Connection#createDispatcher(MessageHandler) createDispatcher} for
     * information about creating an asynchronous subscription with callbacks.
     * 
     * <p>As of 2.6.1 this method will throw an IllegalArgumentException if the subject contains whitespace.
     * 
     * @param subject the subject to subscribe to
     * @param queue the queue group to join
     * @param options subscription options
     * @return an object representing the subscription
     * @throws TimeoutException if the NATS server does not return a response
     * @throws InterruptedException if the thread is interrupted
     * @throws IOException if there are communcation issues with the NATS server
     */    
    public Subscription subscribe(String subject, String queue, SubscribeOptions options) throws InterruptedException, TimeoutException, IOException;

    /**
     * Create a subscription to the specified subject under the control of the
     * specified dispatcher. Since a MessageHandler is also required, the Dispatcher will
     * not prevent duplicate subscriptions from being made.
     *
     * @param subject The subject to subscribe to
     * @param handler The target for the messages
     * @param dispatcher The dispatcher to handle this subscription
     * @return The Subscription, so subscriptions may be later unsubscribed manually.
     * @throws TimeoutException if communication with the NATS server timed out
     * @throws InterruptedException if communication with the NATS was interrupted
     * @throws IOException if there are communcation issues with the NATS server
     * @throws IllegalStateException if the dispatcher was previously closed
     */      
    public Subscription subscribe(String subject, Dispatcher dispatcher, MessageHandler handler) throws InterruptedException, TimeoutException, IOException;

    /**
     * Create a subscription to the specified subject under the control of the
     * specified dispatcher. Since a MessageHandler is also required, the Dispatcher will
     * not prevent duplicate subscriptions from being made.
     *
     * @param subject The subject to subscribe to.
     * @param dispatcher The dispatcher to handle this subscription
     * @param handler The target for the messages
     * @param options The options for this subscription.
     * @return The Subscription, so subscriptions may be later unsubscribed manually.
     * @throws TimeoutException if communication with the NATS server timed out
     * @throws InterruptedException if communication with the NATS was interrupted
     * @throws IOException if there are communcation issues with the NATS server
     * @throws IllegalStateException if the dispatcher was previously closed
     */    
    public Subscription subscribe(String subject, Dispatcher dispatcher, MessageHandler handler, SubscribeOptions options) throws InterruptedException, TimeoutException, IOException;

    /**
     * Create a subscription to the specified subject under the control of the
     * specified dispatcher. Since a MessageHandler is also required, the Dispatcher will
     * not prevent duplicate subscriptions from being made.
     *
     * @param subject The subject to subscribe to.
     * @param queue The queue group to join.
     * @param dispatcher The dispatcher to handle this subscription
     * @param handler The target for the messages
     * @return The Subscription, so subscriptions may be later unsubscribed manually.
     * @throws TimeoutException if communication with the NATS server timed out
     * @throws InterruptedException if communication with the NATS was interrupted
     * @throws IOException if there are communcation issues with the NATS server
     * @throws IllegalStateException if the dispatcher was previously closed
     */      
    public Subscription subscribe(String subject, String queue, Dispatcher dispatcher, MessageHandler handler) throws InterruptedException, TimeoutException, IOException;  

    /**
     * Create a subscription to the specified subject under the control of the
     * specified dispatcher. Since a MessageHandler is also required, the Dispatcher will
     * not prevent duplicate subscriptions from being made.
     *
     * @param subject The subject to subscribe to.
     * @param queue The queue group to join.
     * @param dispatcher The dispatcher to handle this subscription
     * @param handler The target for the messages
     * @param options The options for this subscription.
     * @return The Subscription, so subscriptions may be later unsubscribed manually.
     * @throws TimeoutException if communication with the NATS server timed out
     * @throws InterruptedException if communication with the NATS was interrupted
     * @throws IOException if there are communcation issues with the NATS server
     * @throws IllegalStateException if the dispatcher was previously closed
     */      
    public Subscription subscribe( String subject, String queue, Dispatcher dispatcher, MessageHandler handler, SubscribeOptions options) throws InterruptedException, TimeoutException, IOException;
}
