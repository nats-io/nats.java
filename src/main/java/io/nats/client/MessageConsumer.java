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
 * SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
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
     * There still may be messages available and coming across the wire.
     * @param timeout The time to wait for the stop to succeed, pass 0 to wait
     *                forever. Stop involves moving messages to and from the server
     *                so a very short timeout is not recommended.
     * @throws InterruptedException if one is thrown, in order to propagate it up
     */
    void stop(long timeout) throws InterruptedException;
}
