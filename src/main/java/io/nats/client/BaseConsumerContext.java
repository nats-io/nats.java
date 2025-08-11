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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.time.Duration;

/**
 * The Consumer Context provides a convenient interface around a defined JetStream Consumer
 * <p> Note: ConsumerContext requires a <b>pull consumer</b>.
 */
public interface BaseConsumerContext {
    /**
     * Gets the consumer name that was used to create the context.
     * Some implementations do not guarantee that the name is available.
     * @return the consumer name
     */
    @Nullable
    String getConsumerName();

    /**
     * Read the next message with max wait set to {@value BaseConsumeOptions#DEFAULT_EXPIRES_IN_MILLIS} ms
     * @return the next message or null if the max wait expires
     * @throws IOException covers various communication issues with the NATS
     *         server, such as timeout or interruption
     * @throws InterruptedException if one is thrown, to propagate it up
     * @throws JetStreamStatusCheckedException an exception representing a status that requires attention,
     *         such as the consumer was deleted on the server in the middle of use.
     * @throws JetStreamApiException the request had an error related to the data
     */
    @Nullable
    Message next() throws IOException, InterruptedException, JetStreamStatusCheckedException, JetStreamApiException;

    /**
     * Read the next message with provided max wait
     * @param maxWait duration of max wait.
     *                If null, zero or negative, {@value BaseConsumeOptions#DEFAULT_EXPIRES_IN_MILLIS} will be used.
     *                If greater than zero, it cannot be less than {@value BaseConsumeOptions#MIN_EXPIRES_MILLS} milliseconds.
     * @return the next message or null if the max wait expires
     * @throws IOException covers various communication issues with the NATS
     *         server, such as timeout or interruption
     * @throws InterruptedException if one is thrown, to propagate it up
     * @throws JetStreamStatusCheckedException an exception representing a status that requires attention,
     *         such as the consumer was deleted on the server in the middle of use.
     * @throws JetStreamApiException the request had an error related to the data
     * @throws IllegalArgumentException if maxWait is provided and less than {@value BaseConsumeOptions#MIN_EXPIRES_MILLS}
     */
    @Nullable
    Message next(@Nullable Duration maxWait) throws IOException, InterruptedException, JetStreamStatusCheckedException, JetStreamApiException;

    /**
     * Read the next message with provided max wait
     * @param maxWaitMillis the max wait value in milliseconds. Cannot be less than {@value BaseConsumeOptions#MIN_EXPIRES_MILLS} milliseconds.
     * @return the next message or null if the max wait expires
     * @throws IOException covers various communication issues with the NATS
     *         server, such as timeout or interruption
     * @throws InterruptedException if one is thrown, to propagate it up
     * @throws JetStreamStatusCheckedException an exception representing a status that requires attention,
     *         such as the consumer was deleted on the server in the middle of use.
     * @throws JetStreamApiException the request had an error related to the data
     * @throws IllegalArgumentException if maxWait is provided and less than {@value BaseConsumeOptions#MIN_EXPIRES_MILLS}
     */
    @Nullable
    Message next(long maxWaitMillis) throws IOException, InterruptedException, JetStreamStatusCheckedException, JetStreamApiException;

    /**
     * Start a one use Fetch Consumer using all defaults other than the number of messages. See {@link FetchConsumer}
     * @param maxMessages the maximum number of messages to consume
     * @return the FetchConsumer instance
     * @throws IOException covers various communication issues with the NATS
     *         server, such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    @NonNull
    FetchConsumer fetchMessages(int maxMessages) throws IOException, JetStreamApiException;

    /**
     * Start a one use Fetch Consumer using all defaults other than the number of bytes. See {@link FetchConsumer}
     * @param maxBytes the maximum number of bytes to consume
     * @return the FetchConsumer instance
     * @throws IOException covers various communication issues with the NATS
     *         server, such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    @NonNull
    FetchConsumer fetchBytes(int maxBytes) throws IOException, JetStreamApiException;

    /**
     * Start a one-use Fetch Consumer with custom FetchConsumeOptions. See {@link FetchConsumeOptions}
     * @param fetchConsumeOptions the custom fetch consume options.
     * @return the FetchConsumer instance
     * @throws IOException covers various communication issues with the NATS
     *         server, such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    @NonNull
    FetchConsumer fetch(@NonNull FetchConsumeOptions fetchConsumeOptions) throws IOException, JetStreamApiException;

    /**
     * Start a long-running IterableConsumer with default ConsumeOptions. See {@link IterableConsumer} and {@link ConsumeOptions}
     * IterableConsumer require the developer calls nextMessage.
     * @return the IterableConsumer instance
     * @throws IOException covers various communication issues with the NATS
     *         server, such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    @NonNull
    IterableConsumer iterate() throws IOException, JetStreamApiException;

    /**
     * Start a long-running IterableConsumer with custom ConsumeOptions. See {@link IterableConsumer} and {@link ConsumeOptions}
     * IterableConsumer requires the developer calls nextMessage.
     * @param consumeOptions the custom consume options
     * @return the IterableConsumer instance
     * @throws IOException covers various communication issues with the NATS
     *         server, such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    @NonNull
    IterableConsumer iterate(@NonNull ConsumeOptions consumeOptions) throws IOException, JetStreamApiException;

    /**
     * Start a long-running MessageConsumer with default ConsumeOptions. See {@link MessageConsumer} and  {@link ConsumeOptions}
     * and the default dispatcher for this consumer context.
     * @param handler the MessageHandler used for receiving messages.
     * @return the MessageConsumer instance
     * @throws IOException covers various communication issues with the NATS
     *         server, such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    @NonNull
    MessageConsumer consume(@NonNull MessageHandler handler) throws IOException, JetStreamApiException;

    /**
     * Start a long-running MessageConsumer with default ConsumeOptions. See {@link MessageConsumer} and  {@link ConsumeOptions}
     * @param dispatcher The dispatcher to handle this subscription. If null, the default dispatcher will be used.
     * @param handler the MessageHandler used for receiving messages.
     * @return the MessageConsumer instance
     * @throws IOException covers various communication issues with the NATS
     *         server, such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    @NonNull
    MessageConsumer consume(@Nullable Dispatcher dispatcher, @NonNull MessageHandler handler) throws IOException, JetStreamApiException;

    /**
     * Start a long-running MessageConsumer with custom ConsumeOptions. See {@link MessageConsumer} and  {@link ConsumeOptions}
     * and the default dispatcher for this consumer context.
     * @param consumeOptions the custom consume options
     * @param handler the MessageHandler used for receiving messages.
     * @return the MessageConsumer instance
     * @throws IOException covers various communication issues with the NATS
     *         server, such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    @NonNull
    MessageConsumer consume(@NonNull ConsumeOptions consumeOptions, @NonNull MessageHandler handler) throws IOException, JetStreamApiException;

    /**
     * Start a long-running MessageConsumer with custom ConsumeOptions. See {@link MessageConsumer} and  {@link ConsumeOptions}
     * @param consumeOptions the custom consume options
     * @param dispatcher The dispatcher to handle this subscription. If null, the default dispatcher will be used.
     * @param handler the MessageHandler used for receiving messages.
     * @return the MessageConsumer instance
     * @throws IOException covers various communication issues with the NATS
     *         server, such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    @NonNull
    MessageConsumer consume(@NonNull ConsumeOptions consumeOptions, @Nullable Dispatcher dispatcher, @NonNull MessageHandler handler) throws IOException, JetStreamApiException;
}
