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
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.StreamInfo;
import io.nats.client.api.StreamInfoOptions;

import java.io.IOException;

/**
 * The Stream Context provide a set of operations for managing the stream
 * and its contents and managing consumers.
 * SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
 */
public interface StreamContext {
    /**
     * Gets the stream name that was used to create the context.
     * @return the stream name
     */
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
    StreamInfo getStreamInfo(StreamInfoOptions options) throws IOException, JetStreamApiException;

    /**
     * Management function to creates a consumer on this stream.
     * @param config the consumer configuration to use.
     * @return consumer information.
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    ConsumerInfo createConsumer(ConsumerConfiguration config) throws IOException, JetStreamApiException;

    /**
     * Management function to deletes a consumer.
     * @param consumerName the name of the consumer.
     * @return true if the delete succeeded
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data, for instance the consumer does not exist.
     */
    boolean deleteConsumer(String consumerName) throws IOException, JetStreamApiException;

    /**
     * Create a consumer context for on the context's stream and specific named consumer.
     * Verifies that the consumer exists.
     * @param consumerName the name of the consumer
     * @return a ConsumerContext object
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    ConsumerContext getConsumerContext(String consumerName) throws IOException, JetStreamApiException;
}
