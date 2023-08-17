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

import io.nats.client.api.ConsumerInfo;

import java.io.IOException;

/**
 * The MessageConsumer interface is the core interface replacing
 * a subscription for a simplified consumer.
 */
public interface MessageConsumer extends AutoCloseable {
    /**
     * Gets information about the consumer behind this subscription.
     * @return consumer information
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    ConsumerInfo getConsumerInfo() throws IOException, JetStreamApiException;

    /**
     * Gets information about the consumer behind this subscription.
     * This returns the last read version of Consumer Info, which could technically be out of date.
     * @return consumer information
     */
    ConsumerInfo getCachedConsumerInfo();

    /**
     * Stop the MessageConsumer from asking for any more messages from the server.
     * The consumer will finish all pull request already in progress, but will not start any new ones.
     */
    void stop();

    /**
     * Stopped indicates whether consuming has been stopped. Can be stopped without being finished.
     * @return the stopped flag
     */
    boolean isStopped();

    /**
     * Finish indicates all messages have been received from the server. Can be finished without being stopped.
     * @return the finished flag
     */
    boolean isFinished();
}
