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
import io.nats.client.support.JsonUtils;

import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.objectString;

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
        super(json);

        String jsonObject = JsonUtils.getJsonObject(CONFIG, json);
        this.configuration = new ConsumerConfiguration(jsonObject);

        // both config and the base have a name field
        JsonUtils.removeObject(json, CONFIG);

        stream = JsonUtils.readString(json, STREAM_NAME_RE);
        name = JsonUtils.readString(json, NAME_RE);
        created = JsonUtils.readDate(json, CREATED_RE);

        jsonObject = JsonUtils.getJsonObject(DELIVERED, json);
        this.delivered = new SequenceInfo(jsonObject);

        jsonObject = JsonUtils.getJsonObject(ACK_FLOOR, json);
        this.ackFloor = new SequenceInfo(jsonObject);

        numAckPending = JsonUtils.readLong(json, NUM_ACK_PENDING_RE, 0);
        numRedelivered = JsonUtils.readLong(json, NUM_REDELIVERED_RE, 0);
        numPending = JsonUtils.readLong(json, NUM_PENDING_RE, 0);
        numWaiting = JsonUtils.readLong(json, NUM_WAITING_RE, 0);

        clusterInfo = ClusterInfo.optionalInstance(json);
        pushBound = JsonUtils.readBoolean(json, PUSH_BOUND_RE);
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
