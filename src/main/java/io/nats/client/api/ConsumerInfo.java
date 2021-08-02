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
    private final SequencePair delivered;
    private final SequencePair ackFloor;
    private final long numPending;
    private final long numWaiting;
    private final long numAckPending;
    private final long numRedelivered;

    public ConsumerInfo(Message msg) {
        this(new String(msg.getData(), StandardCharsets.UTF_8));
    }

    public ConsumerInfo(String json) {
        super(json);
        stream = JsonUtils.readString(json, STREAM_NAME_RE);
        name = JsonUtils.readString(json, NAME_RE);
        created = JsonUtils.readDate(json, CREATED_RE);

        String jsonObject = JsonUtils.getJsonObject(CONFIG, json);
        this.configuration = new ConsumerConfiguration(jsonObject);

        jsonObject = JsonUtils.getJsonObject(DELIVERED, json);
        this.delivered = new SequencePair(jsonObject);

        jsonObject = JsonUtils.getJsonObject(ACK_FLOOR, json);
        this.ackFloor = new SequencePair(jsonObject);

        numAckPending = JsonUtils.readLong(json, NUM_ACK_PENDING_RE, 0);
        numRedelivered = JsonUtils.readLong(json, NUM_REDELIVERED_RE, 0);
        numPending = JsonUtils.readLong(json, NUM_PENDING_RE, 0);
        numWaiting = JsonUtils.readLong(json, NUM_WAITING_RE, 0);
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

    public SequencePair getDelivered() {
        return delivered;
    }

    public SequencePair getAckFloor() {
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

    @Override
    public String toString() {
        return "ConsumerInfo{" +
                "stream='" + stream + '\'' +
                ", name='" + name + '\'' +
                ", numPending=" + numPending +
                ", numWaiting=" + numWaiting +
                ", numAckPending=" + numAckPending +
                ", numRedelivered=" + numRedelivered +
                ", created=" + created +
                ", " + objectString("delivered", delivered) +
                ", " + objectString("ackFloor", ackFloor) +
                ", " + objectString("configuration", configuration) +
                '}';
    }
}
