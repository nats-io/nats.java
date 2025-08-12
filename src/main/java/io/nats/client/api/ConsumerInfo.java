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

package io.nats.client.api;

import io.nats.client.Message;
import io.nats.client.support.DateTimeUtils;
import io.nats.client.support.JsonValue;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.time.ZonedDateTime;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonValueUtils.*;
import static io.nats.client.support.NatsConstants.UNDEFINED;

/**
 * The ConsumerInfo class returns information about a JetStream consumer.
 */
public class ConsumerInfo extends ApiResponse<ConsumerInfo> {

    private final String stream;
    private final String name;
    private final ConsumerConfiguration configuration;
    private final ZonedDateTime created;
    private final SequenceInfo delivered;
    private final SequenceInfo ackFloor;
    private final long numPending;
    private final long numWaiting;
    private final long numAckPending;
    private final long numRedelivered;
    private final boolean paused;
    private final Duration pauseRemaining;
    private final ClusterInfo clusterInfo;
    private final boolean pushBound;
    private final ZonedDateTime timestamp;

    public ConsumerInfo(Message msg) {
        this(parseMessage(msg));
    }

    public ConsumerInfo(JsonValue vConsumerInfo) {
        super(vConsumerInfo);
        if (hasError()) {
            this.configuration = ConsumerConfiguration.builder().build();
            stream = UNDEFINED;
            name = UNDEFINED;
            created = DateTimeUtils.DEFAULT_TIME;
            delivered = null;
            ackFloor = null;
            numAckPending = -1;
            numRedelivered = -1;
            numPending = -1;
            numWaiting = -1;
            paused = false;
            pauseRemaining = null;
            clusterInfo = null;
            pushBound = false;
            timestamp = null;
        }
        else {
            JsonValue jvConfig = nullValueIsError(jv, CONFIG, JsonValue.EMPTY_MAP) ;
            configuration = ConsumerConfiguration.builder().jsonValue(jvConfig).build();

            stream = nullStringIsError(jv, STREAM_NAME);
            name = nullStringIsError(jv, NAME);
            created = nullDateIsError(jv, CREATED);

            delivered = new SequenceInfo(readObject(jv, DELIVERED));
            ackFloor = new SequenceInfo(readObject(jv, ACK_FLOOR));

            numAckPending = readLong(jv, NUM_ACK_PENDING, 0);
            numRedelivered = readLong(jv, NUM_REDELIVERED, 0);
            numPending = readLong(jv, NUM_PENDING, 0);
            numWaiting = readLong(jv, NUM_WAITING, 0);
            paused = readBoolean(jv, PAUSED, false);
            pauseRemaining = readNanos(jv, PAUSE_REMAINING);

            clusterInfo = ClusterInfo.optionalInstance(readValue(jv, CLUSTER));
            pushBound = readBoolean(jv, PUSH_BOUND);

            timestamp = readDate(jv, TIMESTAMP);
        }
    }

    /**
     * The consumer configuration representing this consumer.
     * @return the config
     */
    @NonNull
    public ConsumerConfiguration getConsumerConfiguration() {
        return configuration;
    }

    /**
     * A unique name for the consumer, either machine generated or the durable name
     * @return the name
     */
    @NonNull
    public String getName() {
        return name;
    }

    /**
     * The Stream the consumer belongs to
     * @return the stream name
     */
    @NonNull
    public String getStreamName() {
        return stream;
    }

    /**
     * Gets the creation time of the consumer.
     * @return the creation date and time.
     */
    @NonNull
    public ZonedDateTime getCreationTime() {
        return created;
    }

    /**
     * The last message delivered from this Consumer
     * @return the last delivered sequence info
     */
    @NonNull
    public SequenceInfo getDelivered() {
        return delivered;
    }

    /**
     * The highest contiguous acknowledged message
     * @return the sequence info
     */
    @NonNull
    public SequenceInfo getAckFloor() {
        return ackFloor;
    }

    /**
     * The number of messages left unconsumed in this Consumer
     * @return the number of pending messages
     */
    public long getNumPending() {
        return numPending;
    }

    /**
     * The number of pull consumers waiting for messages
     * @return the number of waiting messages
     */
    public long getNumWaiting() {
        return numWaiting;
    }

    /**
     * The number of messages pending acknowledgement
     * @return the number of messages
     */
    public long getNumAckPending() {
        return numAckPending;
    }

    /**
     * The number of redeliveries that have been performed
     * @return the number of redeliveries
     */
    public long getRedelivered() {
        return numRedelivered;
    }

    /**
     * Indicates if the consumer is currently in a paused state
     * @return true if paused
     */
    public boolean getPaused() {
        return paused;
    }

    /**
     * When paused the time remaining until unpause
     * @return the time remaining
     */
    @Nullable
    public Duration getPauseRemaining() {
        return pauseRemaining;
    }

    /**
     * Information about the cluster for clustered environments
     * @return the cluster info object
     */
    @Nullable
    public ClusterInfo getClusterInfo() {
        return clusterInfo;
    }

    /**
     * Indicates if any client is connected and receiving messages from a push consumer
     * @return the flag
     */
    public boolean isPushBound() {
        return pushBound;
    }

    /**
     * Gets the server time the info was gathered
     * @return the server gathered timed
     */
    @Nullable
    public ZonedDateTime getTimestamp() {
        return timestamp;
    }

    /**
     * A way to more accurately calculate pending during the initial state
     * of the consumer when messages may be unaccounted for in flight
     * @return the calculated amount
     */
    public long getCalculatedPending() {
        return numPending + delivered.getConsumerSequence();
    }
}
