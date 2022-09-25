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

import io.nats.client.api.PublishAck;
import io.nats.client.api.SimpleConsumerConfiguration;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * JetStream context for creation and access to streams and consumers in NATS.
 */
public interface JetStream2 {

    /**
     * Send a message to the specified subject and waits for a response from
     * Jetstream. The default publish options will be used.
     * The message body <strong>will not</strong> be copied. The expected
     * usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * js.publish("destination", "message".getBytes("UTF-8"))
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
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    PublishAck publish(String subject, byte[] body) throws IOException, JetStreamApiException;

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
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    PublishAck publish(String subject, byte[] body, PublishOptions options) throws IOException, JetStreamApiException;

    /**
     * Send a message to the specified subject and waits for a response from
     * Jetstream. The default publish options will be used.
     * The message body <strong>will not</strong> be copied. The expected
     * usage with string content is something like:
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
     * @return The publish acknowledgement
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    PublishAck publish(Message message) throws IOException, JetStreamApiException;

    /**
     * Send a message to the specified subject and waits for a response from
     * Jetstream. The message body <strong>will not</strong> be copied. The expected
     * usage with string content is something like:
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
     * @return The publish acknowledgement
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    PublishAck publish(Message message, PublishOptions options) throws IOException, JetStreamApiException;

    /**
     * Send a message to the specified subject but does not wait for a response from
     * Jetstream. The default publish options will be used.
     * The message body <strong>will not</strong> be copied. The expected
     * usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * CompletableFuture&lt;PublishAck&gt; future =
     *     js.publishAsync("destination", "message".getBytes("UTF-8"),)
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     *
     * See {@link #publish(String, byte[]) publish()} for more details on
     * publish during reconnect.
     *
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
     * Jetstream. The message body <strong>will not</strong> be copied. The expected
     * usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * CompletableFuture&lt;PublishAck&gt; future =
     *     js.publishAsync("destination", "message".getBytes("UTF-8"), publishOptions)
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     *
     * See {@link #publish(String, byte[]) publish()} for more details on
     * publish during reconnect.
     *
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
     * Jetstream. The default publish options will be used.
     * The message body <strong>will not</strong> be copied. The expected
     * usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * CompletableFuture&lt;PublishAck&gt; future = js.publishAsync(message)
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     *
     * See {@link #publish(String, byte[]) publish()} for more details on
     * publish during reconnect.
     *
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
     * Jetstream. The message body <strong>will not</strong> be copied. The expected
     * usage with string content is something like:
     *
     * <pre>
     * nc = Nats.connect()
     * JetStream js = nc.JetStream()
     * CompletableFuture&lt;PublishAck&gt; future = js.publishAsync(message, publishOptions)
     * </pre>
     *
     * where the sender creates a byte array immediately before calling publish.
     *
     * See {@link #publish(String, byte[]) publish()} for more details on
     * publish during reconnect.
     *
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

    JetStreamReader read(String stream, String consumerName) throws IOException, JetStreamApiException;
    JetStreamReader read(String stream, String consumerName, SimpleConsumerOptions options) throws IOException, JetStreamApiException;
    JetStreamReader read(String stream, SimpleConsumerConfiguration consumerConfiguration) throws IOException, JetStreamApiException;
    JetStreamReader read(String stream, SimpleConsumerConfiguration consumerConfiguration, SimpleConsumerOptions options) throws IOException, JetStreamApiException;

    SimpleConsumer listen(String stream, String consumerName, MessageHandler handler) throws IOException, JetStreamApiException;
    SimpleConsumer listen(String stream, String consumerName, MessageHandler handler, SimpleConsumerOptions options) throws IOException, JetStreamApiException;
    SimpleConsumer listen(String stream, SimpleConsumerConfiguration consumerConfiguration, MessageHandler handler) throws IOException, JetStreamApiException;
    SimpleConsumer listen(String stream, SimpleConsumerConfiguration consumerConfiguration, MessageHandler handler, SimpleConsumerOptions options) throws IOException, JetStreamApiException;
}
