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

import io.nats.client.jetstream.StreamState;
import io.nats.client.support.JsonUtils;
import io.nats.client.support.JsonUtils.FieldType;

import java.time.ZonedDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The StreamInfo class contains information about a jetstream stream.
 */
public class StreamInfo {

    private final StreamConfiguration config;
    private final ZonedDateTime created;
    private final StreamState state;
    
    private static final String createdField =  "created";
    private static final Pattern createdRE = JsonUtils.buildPattern(createdField, FieldType.jsonString);

    /**
     * Internal method to generate consumer information.
     * @param json JSON represeenting the consumer information.
     */
    public StreamInfo(String json) {
        Matcher m = createdRE.matcher(json);
        this.created = m.find() ? JsonUtils.parseDateTime(m.group(1)) : null;
        this.config = new StreamConfiguration(JsonUtils.getJSONObject("config", json));
        this.state = new StreamState(JsonUtils.getJSONObject("state", json));
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
}
