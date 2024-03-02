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
import io.nats.client.support.JsonValue;

import java.time.Duration;
import java.time.ZonedDateTime;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonValueUtils.*;

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
        this.configuration = new ConsumerConfiguration(readObject(jv, CONFIG));

        stream = readString(jv, STREAM_NAME);
        name = readString(jv, NAME);
        created = readDate(jv, CREATED);

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

    public ConsumerConfiguration getConsumerConfiguration() {
        return configuration;
    }

    public String getName() {
        return name;
    }

    public String getStreamName() {
        return stream;
    }

    /**
     * Gets the creation time of the consumer.
     * @return the creation date and time.
     */
    public ZonedDateTime getCreationTime() {
        return created;
    }

    public SequenceInfo getDelivered() {
        return delivered;
    }

    public SequenceInfo getAckFloor() {
        return ackFloor;
    }

    public long getNumPending() {
        return numPending;
    }

    public long getNumWaiting() {
        return numWaiting;
    }

    public long getNumAckPending() {
        return numAckPending;
    }

    public long getRedelivered() {
        return numRedelivered;
    }

    public boolean getPaused() {
        return paused;
    }

    public Duration getPauseRemaining() {
        return pauseRemaining;
    }

    public ClusterInfo getClusterInfo() {
        return clusterInfo;
    }

    public boolean isPushBound() {
        return pushBound;
    }

    /**
     * Gets the server time the info was gathered
     * @return the server gathered timed
     */
    public ZonedDateTime getTimestamp() {
        return timestamp;
    }

    public long getCalculatedPending() {
        return numPending + delivered.getConsumerSequence();
    }
}
