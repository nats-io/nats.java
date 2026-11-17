// Copyright 2020-2026 The NATS Authors
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
     * Gets the consumer name associated with the subscription.
     * For simplified consumers, this value can be null unless
     * the consumer info was manually read via {@link #getConsumerInfo()}.
     * @return the consumer name
     */
    String getConsumerName();

    /**
     * Gets information about the consumer behind this subscription.
     * @return consumer information
     * @throws IOException covers various communication issues with the NATS
     *         server, such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    ConsumerInfo getConsumerInfo() throws IOException, JetStreamApiException;

    /**
     * Gets information about the consumer behind this subscription.
     * This returns the last read version of Consumer Info,
     * which could be null or out of date.
     * @return consumer information
     */
    ConsumerInfo getCachedConsumerInfo();

    /**
     * Stop will prevent any new pull requests. The pull requests already in progress are allowed to finish,
     * and every message received for them is delivered to the handler or returned from nextMessage.
     * Once that is done the consumer is unsubscribed and {@link #isFinished()} is set to true.
     * Use {@link #close()} to unsubscribe immediately.
     */
    void stop();

    /**
     * Unsubscribe the underlying subject. Close will be lenient. In flight and buffered messages may still be delivered.
     */
    @Override
	void close() throws Exception;

    /**
     * Stopped indicates whether consuming has been stopped. Can be stopped without being finished.
     * @return the stopped flag
     */
    boolean isStopped();

    /**
     * Finished indicates all messages requested from the server have been received and handled,
     * meaning delivered to the handler or returned from nextMessage. Can be finished without being stopped.
     * @return the finished flag
     */
    boolean isFinished();
}
