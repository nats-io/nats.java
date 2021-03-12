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

package io.nats.client.impl;

import io.nats.client.Message;

import java.time.ZonedDateTime;

import static io.nats.client.support.ApiConstants.*;

/**
 * The StreamInfo class contains information about a JetStream stream.
 */
public class StreamInfo extends JetStreamApiResponse<StreamInfo> {

    private final ZonedDateTime created;
    private final StreamConfiguration config;
    private final StreamState state;

    public StreamInfo(Message msg) {
        this(JsonUtils.decode(msg.getData()));
    }

    StreamInfo(String json) {
        super(json);
        this.created = JsonUtils.readDate(json, CREATED_RE);
        this.config = StreamConfiguration.fromJson(JsonUtils.getJSONObject(CONFIG, json));
        this.state = new StreamState(JsonUtils.getJSONObject(STATE, json));
    }
    
    /**
     * Gets the stream configuration.
     * @return the stream configuration.
     */
    public StreamConfiguration getConfiguration() {
        return config;
    }

    /**
     * Gets the stream state.
     * @return the stream state
     */
    public StreamState getStreamState() {
        return state;
    }

    /**
     * Gets the creation time of the stream.
     * @return the creation date and time.
     */
    public ZonedDateTime getCreateTime() {
        return created;
    }

    @Override
    public String toString() {
        return "StreamInfo{" +
                "created=" + created +
                ", " + config +
                ", " + state +
                '}';
    }
}
