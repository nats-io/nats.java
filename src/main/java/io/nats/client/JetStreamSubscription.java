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

import io.nats.client.impl.JetStreamApiException;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

/**
 * Subscription on a JetStream context.
 */
public interface JetStreamSubscription extends Subscription {
    /**
     * The maximum pull size
     */
    public static final int MAX_PULL_SIZE = 256;

    /**
     * Polls for new messages, overriding the default batch size for this pull only.
     * This should only be used when the subscription is pull based.
     *
     * @param batchSize the size of the batch
     */
    void pull(int batchSize);

    /**
     * Polls for new messages in noWait mode with the specified batch size.
     * When true a response with a 404 status header will be returned
     * when no messages are available.
     * This should only be used when the subscription is pull based.
     *
     * @param batchSize the size of the batch
     */
    void pullNoWait(int batchSize);

    /**
     * Polls for new messages, sets an expire time for this pull only.
     * This should only be used when the subscription is pull based.
     *
     * @param batchSize the size of the batch
     * @param expiresIn how long from now this request should be expired from the server wait list
     */
    void pullExpiresIn(int batchSize, Duration expiresIn);

    /**
     *
     * @param batchSize the size of the batch
     * @param timeout the timeout
     */
    List<Message> fetch(int batchSize, Duration timeout);

    /**
     * Gets information about the consumer behind this subscription.
     * @return consumer information
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    ConsumerInfo getConsumerInfo() throws IOException, JetStreamApiException;
}
