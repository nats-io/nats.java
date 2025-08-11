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

import io.nats.client.api.*;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.util.List;

/**
 * The Stream Context provide a set of operations for managing the stream
 * and its contents and for managing consumers.
 * <p> For basic usage examples see {@link JetStream JetStream}
 */
public interface StreamContext {
    /**
     * Gets the stream name that was used to create the context.
     * @return the stream name
     */
    @NonNull
    String getStreamName();

    /**
     * Gets information about the stream for this context.
     * Does not retrieve any optional data.
     * See the overloaded version that accepts StreamInfoOptions
     * @return stream information
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data,
     *         most likely the stream has been removed since the context was created.
     */
    @NonNull
    StreamInfo getStreamInfo() throws IOException, JetStreamApiException;

    /**
     * Gets information about the stream for this context.
     * @param options the stream info options. If null, request will not return any optional data.
     * @return stream information
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data,
     *         most likely the stream has been removed since the context was created.
     */
    @NonNull
    StreamInfo getStreamInfo(@Nullable StreamInfoOptions options) throws IOException, JetStreamApiException;

    /**
     * Purge stream messages
     * @return PurgeResponse the purge response
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    @NonNull
    PurgeResponse purge() throws IOException, JetStreamApiException;

    /**
     * Purge messages for a specific subject
     * @param options the purge options
     * @return PurgeResponse the purge response
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    @NonNull
    PurgeResponse purge(PurgeOptions options) throws IOException, JetStreamApiException;

    /**
     * Get a consumer context for the context's stream and specific named consumer.
     * Verifies that the consumer exists.
     * <p> Note that ConsumerContext expects a <b>pull consumer</b>.
     * @param consumerName the name of the consumer
     * @return a ConsumerContext object
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data.
     */
    @NonNull
    ConsumerContext getConsumerContext(@NonNull String consumerName) throws IOException, JetStreamApiException;

    /**
     * Management function to create or update a consumer on this stream.
     * <p> Note that ConsumerContext expects a <b>pull consumer</b>.
     * @param config the consumer configuration to use.
     * @return a ConsumerContext object
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data.
     */
    @NonNull
    ConsumerContext createOrUpdateConsumer(@NonNull ConsumerConfiguration config) throws IOException, JetStreamApiException;

    /**
     * Create an ordered consumer context for the context's stream.
     * @param config the configuration for the ordered consumer
     * @return an OrderedConsumerContext object
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    @NonNull
    OrderedConsumerContext createOrderedConsumer(@NonNull OrderedConsumerConfiguration config) throws IOException, JetStreamApiException;

    /**
     * Management function to deletes a consumer.
     * @param consumerName the name of the consumer.
     * @return true if the delete succeeded
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data, for instance the consumer does not exist.
     */
    boolean deleteConsumer(@NonNull String consumerName) throws IOException, JetStreamApiException;

    /**
     * Gets the info for an existing consumer.
     * @param consumerName the name of the consumer.
     * @return consumer information
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    @NonNull
    ConsumerInfo getConsumerInfo(@NonNull String consumerName) throws IOException, JetStreamApiException;

    /**
     * Return a list of consumers by name
     * @return The list of names
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    @NonNull
    List<String> getConsumerNames() throws IOException, JetStreamApiException;

    /**
     * Return a list of ConsumerInfo objects.
     * @return The list of ConsumerInfo
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    @NonNull
    List<ConsumerInfo> getConsumers() throws IOException, JetStreamApiException;

    /**
     * Get MessageInfo for the message with the exact sequence in the stream.
     * @param seq the sequence number of the message
     * @return The MessageInfo
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    @NonNull
    MessageInfo getMessage(long seq) throws IOException, JetStreamApiException;

    /**
     * Get MessageInfo for the last message of the subject.
     * @param subject the subject to get the last message for.
     * @return The MessageInfo
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    @NonNull
    MessageInfo getLastMessage(@NonNull String subject) throws IOException, JetStreamApiException;

    /**
     * Get MessageInfo for the first message of the subject.
     * @param subject the subject to get the first message for.
     * @return The MessageInfo
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    @NonNull
    MessageInfo getFirstMessage(@NonNull String subject) throws IOException, JetStreamApiException;

    /**
     * Get MessageInfo for the message of the message sequence
     * is equal to or greater the requested sequence for the subject.
     * @param seq the first possible sequence number of the message
     * @param subject the subject to get the next message for.
     * @return The MessageInfo
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    @NonNull
    MessageInfo getNextMessage(long seq, @NonNull String subject) throws IOException, JetStreamApiException;

    /**
     * Deletes a message, overwriting the message data with garbage
     * This can be considered an expensive (time-consuming) operation, but is more secure.
     * @param seq the sequence number of the message
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @return true if the delete succeeded
     */
    boolean deleteMessage(long seq) throws IOException, JetStreamApiException;

    /**
     * Deletes a message, optionally erasing the content of the message.
     * @param seq the sequence number of the message
     * @param erase whether to erase the message (overwriting with garbage) or only mark it as erased.
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @return true if the delete succeeded
     */
    boolean deleteMessage(long seq, boolean erase) throws IOException, JetStreamApiException;
}
