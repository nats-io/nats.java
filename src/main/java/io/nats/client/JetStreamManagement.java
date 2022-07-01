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

import io.nats.client.api.*;

import java.io.IOException;
import java.util.List;

/**
 * JetStream Management context for creation and access to streams and consumers in NATS.
 */
public interface JetStreamManagement {

    /**
     * Gets the account statistics for the logged in account.
     * @return account statistics
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws IllegalArgumentException the server is not JetStream enabled
     */
    AccountStatistics getAccountStatistics() throws IOException, JetStreamApiException;

    /**
     * Loads or creates a stream.
     * @param config the stream configuration to use.
     * @return stream information
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws IllegalArgumentException the configuration is missing or invalid
     */
    StreamInfo addStream(StreamConfiguration config) throws IOException, JetStreamApiException;

    /**
     * Updates an existing stream.
     * @param config the stream configuration to use.
     * @return stream information
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws IllegalArgumentException the configuration is missing or invalid
     */
    StreamInfo updateStream(StreamConfiguration config) throws IOException, JetStreamApiException;

    /**
     * Deletes an existing stream.
     * @param streamName the stream name to use.
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @return true if the delete succeeded. Usually throws a JetStreamApiException otherwise
     */
    boolean deleteStream(String streamName) throws IOException, JetStreamApiException;

    /**
     * Gets the info for an existing stream.
     * Does not retrieve any optional data.
     * See the overloaded version that accepts StreamInfoOptions
     * @param streamName the stream name to use.
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @return stream information
     */
    StreamInfo getStreamInfo(String streamName) throws IOException, JetStreamApiException;

    /**
     * Gets the info for an existing stream, and include subject or deleted details
     * as defined by StreamInfoOptions.
     * @param streamName the stream name to use.
     * @param options the stream info options. If null, request will not return any optional data.
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @return stream information
     */
    StreamInfo getStreamInfo(String streamName, StreamInfoOptions options) throws IOException, JetStreamApiException;

    /**
     * Purge stream messages
     * @param streamName the stream name to use.
     * @return PurgeResponse the purge response
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    PurgeResponse purgeStream(String streamName) throws IOException, JetStreamApiException;

    /**
     * Purge messages for a specific subject
     * @param streamName the stream name to use.
     * @param options the purge options
     * @return PurgeResponse the purge response
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    PurgeResponse purgeStream(String streamName, PurgeOptions options) throws IOException, JetStreamApiException;

    /**
     * Loads or creates a consumer.
     * @param streamName name of the stream
     * @param config the consumer configuration to use.
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @return consumer information.
     */
    ConsumerInfo addOrUpdateConsumer(String streamName, ConsumerConfiguration config) throws IOException, JetStreamApiException;

    /**
     * Deletes a consumer.
     * @param streamName name of the stream
     * @param consumer the name of the consumer.
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @return true if the delete succeeded
     */
    boolean deleteConsumer(String streamName, String consumer) throws IOException, JetStreamApiException;

    /**
     * Gets the info for an existing consumer.
     * @param streamName name of the stream
     * @param consumer the consumer name to use.
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @return consumer information
     */
    ConsumerInfo getConsumerInfo(String streamName, String consumer) throws IOException, JetStreamApiException;

    /**
     * Return a list of consumers by name
     * @param streamName the name of the stream.
     * @return The list of names
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    List<String> getConsumerNames(String streamName) throws IOException, JetStreamApiException;

    /**
     * Return a list of ConsumerInfo objects.
     * @param streamName the name of the stream.
     * @return The list of ConsumerInfo
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    List<ConsumerInfo> getConsumers(String streamName) throws IOException, JetStreamApiException;

    /**
     * Return a list of stream names
     * @return The list of names
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    List<String> getStreamNames() throws IOException, JetStreamApiException;

    /**
     * Return a list of stream names that contains the requested subject filter
     * @param subjectFilter the subject, including wildcards
     * @return The list of stream names matching the subject filter. May be empty, will not be null.
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    List<String> getStreamNamesBySubjectFilter(String subjectFilter) throws IOException, JetStreamApiException;

    /**
     * Return a list of StreamInfo objects.
     * @return The list of StreamInfo
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    List<StreamInfo> getStreams() throws IOException, JetStreamApiException;

    /**
     * Return an info object about a message
     * @param streamName the name of the stream.
     * @param seq the sequence number of the message
     * @return The MessageInfo
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    MessageInfo getMessage(String streamName, long seq) throws IOException, JetStreamApiException;

    /**
     * Return an info object about the last message for a subject
     * @param streamName the name of the stream.
     * @param subject the subject to get the last message for
     * @return The MessageInfo
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    MessageInfo getLastMessage(String streamName, String subject) throws IOException, JetStreamApiException;

    /**
     * Deletes a message.
     * @param streamName name of the stream
     * @param seq the sequence number of the message
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @return true if the delete succeeded
     */
    boolean deleteMessage(String streamName, long seq) throws IOException, JetStreamApiException;
}
