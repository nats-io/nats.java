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
import io.nats.client.support.JsonParser;

import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.objectString;
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
    private final ClusterInfo clusterInfo;
    private final boolean pushBound;

    public ConsumerInfo(Message msg) {
        this(new String(msg.getData(), StandardCharsets.UTF_8));
    }

    public ConsumerInfo(String json) {
        super(JsonParser.parse(json));
        if (hasError()) {
            stream = null;
            name = null;
            configuration = null;
            created = null;
            delivered = null;
            ackFloor = null;
            numPending = -1;
            numWaiting = -1;
            numAckPending = -1;
            numRedelivered = -1;
            clusterInfo = null;
            pushBound = false;
        }
        else {
            this.configuration = new ConsumerConfiguration(getMappedObjectOrEmpty(jv, CONFIG));

            stream = getMappedString(jv, STREAM_NAME);
            name = getMappedString(jv, NAME);
            created = getMappedDate(jv, CREATED);

            delivered = new SequenceInfo(getMappedObjectOrEmpty(jv, DELIVERED));
            ackFloor = new SequenceInfo(getMappedObjectOrEmpty(jv, ACK_FLOOR));

            numAckPending = getMappedLong(jv, NUM_ACK_PENDING, 0);
            numRedelivered = getMappedLong(jv, NUM_REDELIVERED, 0);
            numPending = getMappedLong(jv, NUM_PENDING, 0);
            numWaiting = getMappedLong(jv, NUM_WAITING, 0);

            clusterInfo = ClusterInfo.optionalInstance(getMappedValue(jv, CLUSTER));
            pushBound = getMappedBoolean(jv, PUSH_BOUND);
        }
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

    public ClusterInfo getClusterInfo() {
        return clusterInfo;
    }

    public boolean isPushBound() {
        return pushBound;
    }

    public long getCalculatedPending() {
        return numPending + delivered.getConsumerSequence();
    }

    @Override
    public String toString() {
        return "ConsumerInfo{" +
                "stream='" + stream + '\'' +
                ", name='" + name + '\'' +
                ", numPending=" + numPending +
                ", numWaiting=" + numWaiting +
                ", numAckPending=" + numAckPending +
                ", numRedelivered=" + numRedelivered +
                ", pushBound=" + pushBound +
                ", created=" + created +
                ", " + objectString("delivered", delivered) +
                ", " + objectString("ackFloor", ackFloor) +
                ", " + objectString("configuration", configuration) +
                ", " + objectString("cluster", clusterInfo) +
                '}';
    }
}
