// Copyright 2020-2023 The NATS Authors
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

import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.Headers;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * JetStream context for access to streams and consumers in NATS.
 *
 * <h3>Basic usage</h3>
 *
 * <p>{@link #publish(String, byte[]) JetStream.Publish} will send a message on the specified subject, waiting for acknowledgement.
 * A  <b>503 No responders</b> error will be received if no stream is listening on said subject.
 *
 * <p>{@link #publishAsync(String, byte[]) PublishAsync} will not wait for acknowledgement but return a {@link CompletableFuture CompletableFuture},
 * which can be checked for acknowledgement at a later point.
 *
 * <p> Use {@link #getStreamContext(String ) getStreamContext(String)} to access a simplified API for <b>consuming/subscribing</b> messages from Jetstream.
 * It is <b>recommened</b> to manage consumers explicitely through {@link StreamContext StreamContext} or {@link JetStreamManagement JetStreamManagement}
 *
 * <p>{@link #subscribe(String)} is a convenience method for implicitly creating a consumer on a stream and receiving messages. This method should be used for ephemeral (not durable) conusmers.
 * It can create a named durable consumers though Options, but we prefer to avoid creating durable consumers implictly.
 * It is <b>recommened</b> to manage consumers explicitely through {@link StreamContext StreamContext} and {@link ConsumerContext ConsumerContext} or {@link JetStreamManagement JetStreamManagement}
 *
 *
 * <h3>Recommended usage for creating streams, consumers, publish and listen on a stream</h3>
 * <pre>
 *  io.nats.client.Connection nc = Nats.connect();
 *
 *  //Setting up a stream and a consumer
 *  JetStreamManagement jsm = nc.jetStreamManagement();
 *  StreamConfiguration sc = StreamConfiguration.builder()
 *    .name("my-stream")
 *    .storageType(StorageType.File)
 *    .subjects("foo.*", "bar.*")
 *    .build();
 *
 *  jsm.addStream(sc);
 *
 *  ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
 *    .durable("my-consumer")
 *    .build();
 *
 *  jsm.createConsumer("my-stream", consumerConfig);
 *
 *  //Listening and publishing
 *  io.nats.client.JetStream js = nc.jetStream();
 *  ConsumerContext consumerContext = js.getConsumerContext("my-stream", "my-consumer");
 *  MessageConsumer mc = consumerContext.consume(
 *    msg -&gt; {
 *      System.out.println("   Received " + msg.getSubject());
 *      msg.ack();
 *    });
 *
 *  js.publish("foo.joe", "Hello World".getBytes());
 *
 *  //Wait a moment, then stop the MessageConsumer
 *  Thread.sleep(3000);
 *  mc.stop();
 *
 *  </pre>
 *
 *	<h3>Recommended usage of asynchronous publishing</h3>
 *
 *  Jetstream messages can be published asynchronously, returning a CompletableFuture.
 *  Note that you need to check the Future eventually otherwise the delivery guarantee is the same a regular {@link Connection#publish(String, byte[]) Connection.Publish}
 *
 *  <p>We are publishing a batch of 100 messages and check for completion afterwards.
 *
 *  <pre>
 *  int COUNT = 100;
 *  java.util.concurrent.CompletableFuture&lt;?&gt;[] acks = new java.util.concurrent.CompletableFuture&lt;?&gt;[COUNT];
 *
 *  for( int i=0; i&lt;COUNT; i++ ) {
 *    acks[i] = js.publishAsync("foo.joe", ("Hello "+i).getBytes());
 *  }
 *
 *  //Acknowledgments may arrive out of sequence, but CompletableFuture is handling this for us.
 *  for( int i=0; i&lt;COUNT; i++ ) {
 *    try {
 *      acks[i].get();
 *    } catch ( Exception e ) {
 *      //Retry or handle error
 *    }
 *  }
 *
 *  //Now we may send anther batch
 *
 * </pre>
 *
 */
public interface JetStream {

    /**
     * Send a message to the specified subject and waits for a response from
     * Jetstream. The default publish options will be used.
     * The expected usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * js.publish("destination", "message".getBytes("UTF-8"))
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     *
     * @param subject the subject to send the message to
     * @param body the message body
     * @return The acknowledgement of the publish
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    PublishAck publish(String subject, byte[] body) throws IOException, JetStreamApiException;

    /**
     * Send a message to the specified subject and waits for a response from
     * Jetstream. The default publish options will be used.
     * The expected usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * Headers h = new Headers().put("foo", "bar");
     * js.publish("destination", h, "message".getBytes("UTF-8"))
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     * See {@link #publish(String, byte[]) publish()} for more details on
     * publish during reconnect.
     *
     * @param subject the subject to send the message to
     * @param headers Optional headers to publish with the message.
     * @param body the message body
     * @return The acknowledgement of the publish
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    PublishAck publish(String subject, Headers headers, byte[] body) throws IOException, JetStreamApiException;

    /**
     * Send a message to the specified subject and waits for a response from
     * Jetstream. The expected usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * js.publish("destination", "message".getBytes("UTF-8"), publishOptions)
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     * See {@link #publish(String, byte[]) publish()} for more details on
     * publish during reconnect.
     *
     * @param subject the subject to send the message to
     * @param body the message body
     * @param options publisher options
     * @return The acknowledgement of the publish
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    PublishAck publish(String subject, byte[] body, PublishOptions options) throws IOException, JetStreamApiException;

    /**
     * Send a message to the specified subject and waits for a response from
     * Jetstream. The expected usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * Headers h = new Headers().put("foo", "bar");
     * js.publish("destination", h, "message".getBytes("UTF-8"), publishOptions)
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     * See {@link #publish(String, byte[]) publish()} for more details on
     * publish during reconnect.
     *
     * @param subject the subject to send the message to
     * @param headers Optional headers to publish with the message.
     * @param body the message body
     * @param options publisher options
     * @return The acknowledgement of the publish
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    PublishAck publish(String subject, Headers headers, byte[] body, PublishOptions options) throws IOException, JetStreamApiException;

    /**
     * Send a message to the specified subject and waits for a response from
     * Jetstream. The default publish options will be used.
     * The expected usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * js.publish(message)
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     *
     * <p>The Message object allows you to set a replyTo, but in publish requests,
     * the replyTo is reserved for internal use as the address for the
     * server to respond to the client with the PublishAck.</p>
     *
     * See {@link #publish(String, byte[]) publish()} for more details on
     * publish during reconnect.
     *
     * @param message the message to send
     * @return The acknowledgement of the publish
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    PublishAck publish(Message message) throws IOException, JetStreamApiException;

    /**
     * Send a message to the specified subject and waits for a response from
     * Jetstream. The expected usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * js.publish(message, publishOptions)
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     *
     * <p>The Message object allows you to set a replyTo, but in publish requests,
     * the replyTo is reserved for internal use as the address for the
     * server to respond to the client with the PublishAck.</p>
     *
     * See {@link #publish(String, byte[]) publish()} for more details on
     * publish during reconnect.
     *
     * @param message the message to send
     * @param options publisher options
     * @return The acknowledgement of the publish
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    PublishAck publish(Message message, PublishOptions options) throws IOException, JetStreamApiException;

    /**
     * Send a message to the specified subject but does not wait for a response from
     * Jetstream. The default publish options will be used.
     * The expected usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * CompletableFuture&lt;PublishAck&gt; future =
     *     js.publishAsync("destination", "message".getBytes("UTF-8"),)
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     * See {@link #publish(String, byte[]) publish()} for more details on
     * publish during reconnect.
     * The future me be completed with an exception, either
     * an IOException covers various communication issues with the NATS server such as timeout or interruption
     * - or - a JetStreamApiException the request had an error related to the data
     *
     * @param subject the subject to send the message to
     * @param body the message body
     * @return The future
     */
    CompletableFuture<PublishAck> publishAsync(String subject, byte[] body);

    /**
     * Send a message to the specified subject but does not wait for a response from
     * Jetstream. The default publish options will be used.
     * The expected usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * Headers h = new Headers().put("foo", "bar");
     * CompletableFuture&lt;PublishAck&gt; future =
     *     js.publishAsync("destination", h, "message".getBytes("UTF-8"),)
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     * See {@link #publish(String, byte[]) publish()} for more details on
     * publish during reconnect.
     * The future me be completed with an exception, either
     * an IOException covers various communication issues with the NATS server such as timeout or interruption
     * - or - a JetStreamApiException the request had an error related to the data
     *
     * @param subject the subject to send the message to
     * @param headers Optional headers to publish with the message.
     * @param body the message body
     * @return The future
     */
    CompletableFuture<PublishAck> publishAsync(String subject, Headers headers, byte[] body);

    /**
     * Send a message to the specified subject but does not wait for a response from
     * Jetstream. The expected usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * CompletableFuture&lt;PublishAck&gt; future =
     *     js.publishAsync("destination", "message".getBytes("UTF-8"), publishOptions)
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     * See {@link #publish(String, byte[]) publish()} for more details on
     * publish during reconnect.
     * The future me be completed with an exception, either
     * an IOException covers various communication issues with the NATS server such as timeout or interruption
     * - or - a JetStreamApiException the request had an error related to the data
     *
     * @param subject the subject to send the message to
     * @param body the message body
     * @param options publisher options
     * @return The future
     */
    CompletableFuture<PublishAck> publishAsync(String subject, byte[] body, PublishOptions options);

    /**
     * Send a message to the specified subject but does not wait for a response from
     * Jetstream. The expected usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * Headers h = new Headers().put("foo", "bar");
     * CompletableFuture&lt;PublishAck&gt; future =
     *     js.publishAsync("destination", h, "message".getBytes("UTF-8"), publishOptions)
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     * See {@link #publish(String, byte[]) publish()} for more details on
     * publish during reconnect.
     * The future me be completed with an exception, either
     * an IOException covers various communication issues with the NATS server such as timeout or interruption
     * - or - a JetStreamApiException the request had an error related to the data
     *
     * @param subject the subject to send the message to
     * @param headers Optional headers to publish with the message.
     * @param body the message body
     * @param options publisher options
     * @return The future
     */
    CompletableFuture<PublishAck> publishAsync(String subject, Headers headers, byte[] body, PublishOptions options);

    /**
     * Send a message to the specified subject but does not wait for a response from
     * Jetstream. The default publish options will be used.
     * The expected usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * CompletableFuture&lt;PublishAck&gt; future = js.publishAsync(message)
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     * See {@link #publish(String, byte[]) publish()} for more details on
     * publish during reconnect.
     * The future me be completed with an exception, either
     * an IOException covers various communication issues with the NATS server such as timeout or interruption
     * - or - a JetStreamApiException the request had an error related to the data
     *
     * <p>The Message object allows you to set a replyTo, but in publish requests,
     * the replyTo is reserved for internal use as the address for the
     * server to respond to the client with the PublishAck.</p>
     *
     * @param message the message to send
     * @return The future
     */
    CompletableFuture<PublishAck> publishAsync(Message message);

    /**
     * Send a message to the specified subject but does not wait for a response from
     * Jetstream. The expected usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * CompletableFuture&lt;PublishAck&gt; future = js.publishAsync(message, publishOptions)
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     * See {@link #publish(String, byte[]) publish()} for more details on
     * publish during reconnect.
     * The future me be completed with an exception, either
     * an IOException covers various communication issues with the NATS server such as timeout or interruption
     * - or - a JetStreamApiException the request had an error related to the data
     *
     * <p>The Message object allows you to set a replyTo, but in publish requests,
     * the replyTo is reserved for internal use as the address for the
     * server to respond to the client with the PublishAck.</p>
     *
     * @param message the message to publish
     * @param options publisher options
     * @return The future
     */
    CompletableFuture<PublishAck> publishAsync(Message message, PublishOptions options);

    /**
     * Create a synchronous subscription to the specified subject with default options.
     *
     * <p>Use the {@link io.nats.client.Subscription#nextMessage(Duration)}
     * method to read messages for this subscription.
     *
     * <p>See {@link io.nats.client.Connection#createDispatcher(MessageHandler) createDispatcher} for
     * information about creating an asynchronous subscription with callbacks.
     *
     * @param subscribeSubject the subject to subscribe to
     * @return The subscription
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    JetStreamSubscription subscribe(String subscribeSubject) throws IOException, JetStreamApiException;

    /**
     * Create a synchronous subscription to the specified subject.
     *
     * <p>Use the {@link io.nats.client.Subscription#nextMessage(Duration)}
     * method to read messages for this subscription.
     *
     * <p>See {@link io.nats.client.Connection#createDispatcher(MessageHandler) createDispatcher} for
     * information about creating an asynchronous subscription with callbacks.
     *
     * @param subscribeSubject the subject to subscribe to.
     *                         Can be null or empty when the options have a ConsumerConfiguration that supplies a filter subject.
     * @param options optional subscription options
     * @return The subscription
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    JetStreamSubscription subscribe(String subscribeSubject, PushSubscribeOptions options) throws IOException, JetStreamApiException;

    /**
     * Create a synchronous subscription to the specified subject.
     *
     * <p>Use the {@link io.nats.client.Subscription#nextMessage(Duration) nextMessage}
     * method to read messages for this subscription.
     *
     * <p>See {@link io.nats.client.Connection#createDispatcher(MessageHandler) createDispatcher} for
     * information about creating an asynchronous subscription with callbacks.
     *
     * @param subscribeSubject the subject to subscribe to
     *                         Can be null or empty when the options have a ConsumerConfiguration that supplies a filter subject.
     * @param queue the optional queue group to join
     * @param options optional subscription options
     * @return The subscription
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    JetStreamSubscription subscribe(String subscribeSubject, String queue, PushSubscribeOptions options) throws IOException, JetStreamApiException;

    /**
     * Create an asynchronous subscription to the specified subject under the control of the
     * specified dispatcher. Since a MessageHandler is also required, the Dispatcher will
     * not prevent duplicate subscriptions from being made.
     *
     * @param subscribeSubject The subject to subscribe to
     * @param dispatcher The dispatcher to handle this subscription
     * @param handler The target for the messages
     * @param autoAck Whether to auto ack
     * @return The subscription
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    JetStreamSubscription subscribe(String subscribeSubject, Dispatcher dispatcher, MessageHandler handler, boolean autoAck) throws IOException, JetStreamApiException;

    /**
     * Create an asynchronous subscription to the specified subject under the control of the
     * specified dispatcher. Since a MessageHandler is also required, the Dispatcher will
     * not prevent duplicate subscriptions from being made.
     * @param subscribeSubject The subject to subscribe to.
     *                         Can be null or empty when the options have a ConsumerConfiguration that supplies a filter subject.
     * @param dispatcher The dispatcher to handle this subscription
     * @param handler The target for the messages
     * @param autoAck Whether to auto ack
     * @param options The options for this subscription.
     * @return The subscription
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    JetStreamSubscription subscribe(String subscribeSubject, Dispatcher dispatcher, MessageHandler handler, boolean autoAck, PushSubscribeOptions options) throws IOException, JetStreamApiException;

    /**
     * Create an asynchronous subscription to the specified subject under the control of the
     * specified dispatcher. Since a MessageHandler is also required, the Dispatcher will
     * not prevent duplicate subscriptions from being made.
     *
     * @param subscribeSubject The subject to subscribe to.
     *                         Can be null or empty when the options have a ConsumerConfiguration that supplies a filter subject.
     * @param queue the optional queue group to join
     * @param dispatcher The dispatcher to handle this subscription
     * @param handler The target for the messages
     * @param autoAck Whether to auto ack
     * @param options optional subscription options
     * @return The subscription
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    JetStreamSubscription subscribe(String subscribeSubject, String queue, Dispatcher dispatcher, MessageHandler handler, boolean autoAck, PushSubscribeOptions options) throws IOException, JetStreamApiException;

    /**
     * Create a subscription to the specified subject in the mode of pull, with additional options
     * @param subscribeSubject The subject to subscribe to
     *                         Can be null or empty when the options have a ConsumerConfiguration that supplies a filter subject.
     * @param options pull subscription options
     * @return The subscription
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    JetStreamSubscription subscribe(String subscribeSubject, PullSubscribeOptions options) throws IOException, JetStreamApiException;

    /**
     * Create an asynchronous subscription to the specified subject in the mode of pull, with additional options.
     *
     * @param subscribeSubject The subject to subscribe to
     *                         Can be null or empty when the options have a ConsumerConfiguration that supplies a filter subject.
     * @param dispatcher The dispatcher to handle this subscription
     * @param handler The target for the messages
     * @param options pull subscription options
     * @return The subscription
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    JetStreamSubscription subscribe(String subscribeSubject, Dispatcher dispatcher, MessageHandler handler, PullSubscribeOptions options) throws IOException, JetStreamApiException;

    /**
     * Get a stream context for a specific named stream. Verifies that the stream exists.
     *
     * <p><b>Recommended usage:</b> {@link StreamContext StreamContext} and {@link ConsumerContext ConsumerContext} are the preferred way to interact with existing streams and consume from streams.
     * {@link JetStreamManagement JetStreamManagement} should be used to create streams and consumers. Note that {@link ConsumerContext#consume ConsumerContext.consume()} only supports both pull consumers.
     *
     * <pre>
     *  nc = Nats.connect();
     *  Jetstream js = nc.jetStream();
	 *  StreamContext streamContext = js.getStreamContext("my-stream");
	 *  ConsumerContext consumerContext = streamContext.getConsumerContext("my-consumer");
	 *  // Or
	 *  // ConsumerContext consumerContext = js.getConsumerContext("my-stream", "my-consumer");
	 *  consumerContext.consume(
	 *    msg -&gt; {
	 *      System.out.println("   Received " + msg.getSubject());
	 *      msg.ack();
	 *    });
	 *  </pre>
     *
     *
     * @param streamName the name of the stream
     * @return a StreamContext object
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    StreamContext getStreamContext(String streamName) throws IOException, JetStreamApiException;

    /**
     * Get a consumer context for a specific named stream and specific named consumer.
     * <p> Note that ConsumerContext expects a <b>pull consumer</b>.
     * <p><b>Recommended usage:</b> See {@link #getStreamContext(String) getStreamContext(String)}
     *
     * Verifies that the stream and consumer exist.
     * @param streamName the name of the stream
     * @param consumerName the name of the consumer
     * @return a ConsumerContext object
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data. E.g. if the consumerName does not represent a pull consumer.
     */
	ConsumerContext getConsumerContext(String streamName, String consumerName) throws IOException, JetStreamApiException;
}
