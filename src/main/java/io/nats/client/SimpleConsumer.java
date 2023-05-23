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
 * The Simple Consumer interface is the core interface replacing
 * a subscription for a simplified consumer.
 * SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
 */
public interface SimpleConsumer {
    /**
     * Gets information about the consumer behind this subscription.
     * @return consumer information
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    ConsumerInfo getConsumerInfo() throws IOException, JetStreamApiException;

    /**
     * Stop the Simple Consumer from asking for any more messages from the server.
     * Messages do not immediately stop
     * @throws InterruptedException if one is thrown, in order to propagate it up
     */
    void stop() throws InterruptedException;

    /**
     * Whether the Simple Consumer is active. Returns true until stop has been called.
     * @return the active state
     */
    boolean isActive();

    /**
     * Whether the Simple Consumer has pending messages to be sent from the server.
     * This does not indicate that messages are coming, just that the server has not fulfilled
     * the current request. Mostly helpful after stop has been called to check if any more messages
     * are coming
     * @return the pending state
     */
    boolean hasPending();
}
