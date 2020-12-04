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

package io.nats.client.jetstream;

import java.time.ZonedDateTime;

/**
 * This interface defined the methods use to get Jetstream metadata about a message,
 * when applicable.
 */
public interface MetaData {

    /**
     * Gets the stream the message is from.
     *
     * @return the stream.
     */
    String getStream();

    /**
     * Gets the consumer that generated this message.
     *
     * @return the consumer.
     */
    String getConsumer();


    /**
     * Gets the number of times this message has been delivered.
     *
     * @return delivered count.
     */
    long deliveredCount();

    /**
     * Gets the stream sequence number of the message.
     *
     * @return sequence number
     */
    long streamSequence();

    /**
     * Gets consumer sequence number of this message.
     *
     * @return sequence number
     */
    long consumerSequence();

    /**
     * Gets the pending count of the consumer.
     *
     * @return pending count
     */
    long pendingCount();

    /**
     * Gets the timestamp of the message.
     *
     * @return the timestamp
     */
    ZonedDateTime timestamp();
}
