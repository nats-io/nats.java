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

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Subscription on a JetStream context.
 */
public interface JetStreamSubscription extends Subscription {

    /**
     * Polls for new messages.  This should only be used when the subscription
     * is pull based.
     */
    void poll();

    /**
     * Gets information about the consumer behind this subscription.
     * @throws IOException if there are communcation issues with the NATS server
     * @throws TimeoutException if the NATS server does not return a response
     * @throws InterruptedException if the thread is interrupted
     * @return consumer information
     */
    ConsumerInfo getConsumerInfo() throws IOException, TimeoutException, InterruptedException;
}
