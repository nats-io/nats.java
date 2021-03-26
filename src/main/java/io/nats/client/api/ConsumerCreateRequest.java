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

import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonUtils;

import static io.nats.client.support.ApiConstants.CONFIG;
import static io.nats.client.support.ApiConstants.STREAM_NAME;
import static io.nats.client.support.JsonUtils.*;

/**
 * Object used to make a request to create a consumer. Used Internally
 */
public class ConsumerCreateRequest implements JsonSerializable {
    private final String streamName;
    private final ConsumerConfiguration config;

    public ConsumerCreateRequest(String streamName, ConsumerConfiguration config) {
        this.streamName = streamName;
        this.config = config;
    }

    public String getStreamName() {
        return streamName;
    }

    public ConsumerConfiguration getConfig() {
        return config;
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();

        addField(sb, STREAM_NAME, streamName);
        JsonUtils.addField(sb, CONFIG, config);

        return endJson(sb).toString();
    }

    @Override
    public String toString() {
        return "ConsumerCreateRequest{" +
                "streamName='" + streamName + '\'' +
                ", " + objectString("config", config) +
                '}';
    }
}
