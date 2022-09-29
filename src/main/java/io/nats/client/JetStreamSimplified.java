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

import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.SimpleConsumerConfiguration;

import java.io.IOException;
import java.util.List;

/**
 * JetStream context for creation and access to streams and consumers in NATS.
 */
public interface JetStreamSimplified {
    // access the original if you want
    JetStream getJetStream();

    SimpleIterateConsumer iterate(String consumerName, int messageLimit) throws IOException, JetStreamApiException;
    SimpleConsumer listen(String consumerName, int messageLimit, MessageHandler handler) throws IOException, JetStreamApiException;

    SimpleIterateConsumer endlessIterate(String consumerName) throws IOException, JetStreamApiException;
    SimpleIterateConsumer endlessIterate(String consumerName, SimpleConsumerOptions options) throws IOException, JetStreamApiException;
    SimpleIterateConsumer endlessIterate(SimpleConsumerConfiguration config) throws IOException, JetStreamApiException;
    SimpleIterateConsumer endlessIterate(SimpleConsumerConfiguration config, SimpleConsumerOptions options) throws IOException, JetStreamApiException;

    SimpleConsumer endlessListen(String consumerName, MessageHandler handler) throws IOException, JetStreamApiException;
    SimpleConsumer endlessListen(String consumerName, MessageHandler handler, SimpleConsumerOptions options) throws IOException, JetStreamApiException;
    SimpleConsumer endlessListen(SimpleConsumerConfiguration config, MessageHandler handler) throws IOException, JetStreamApiException;
    SimpleConsumer endlessListen(SimpleConsumerConfiguration config, MessageHandler handler, SimpleConsumerOptions options) throws IOException, JetStreamApiException;

    /**
     * Creates or updates a consumer.
     * @param config the consumer configuration to use.
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @return consumer information.
     */
    ConsumerInfo addOrUpdateConsumer(SimpleConsumerConfiguration config) throws IOException, JetStreamApiException;

    /**
     * Deletes a consumer.
     * @param consumer the name of the consumer.
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @return true if the delete succeeded
     */
    boolean deleteConsumer(String consumer) throws IOException, JetStreamApiException;

    /**
     * Gets the info for an existing consumer.
     * @param consumer the consumer name to use.
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @return consumer information
     */
    ConsumerInfo getConsumerInfo(String consumer) throws IOException, JetStreamApiException;

    /**
     * Return a list of consumers by name
     * @return The list of names
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    List<String> getConsumerNames() throws IOException, JetStreamApiException;

    /**
     * Return a list of ConsumerInfo objects.
     * @return The list of ConsumerInfo
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    List<ConsumerInfo> getConsumers() throws IOException, JetStreamApiException;

}
