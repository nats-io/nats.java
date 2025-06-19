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
import org.jetbrains.annotations.NotNull;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.*;

/**
 * Object used to make a request to create a consumer. Used Internally
 */
public class ConsumerCreateRequest implements JsonSerializable {
    public enum Action {
        Create("create"),
        Update("update"),
        CreateOrUpdate(null);

        public final String actionText;

        Action(String actionText) {
            this.actionText = actionText;
        }
    }

    private final String streamName;
    private final ConsumerConfiguration config;
    private final Action action;

    public ConsumerCreateRequest(String streamName, ConsumerConfiguration config) {
        this.streamName = streamName;
        this.config = config;
        this.action = Action.CreateOrUpdate;
    }

    public ConsumerCreateRequest(String streamName, ConsumerConfiguration config, Action action) {
        this.streamName = streamName;
        this.config = config;
        this.action = action;
    }

    @NotNull
    public String getStreamName() {
        return streamName;
    }

    @NotNull
    public ConsumerConfiguration getConfig() {
        return config;
    }

    @NotNull
    public Action getAction() {
        return action;
    }

    @Override
    @NotNull
    public String toJson() {
        StringBuilder sb = beginJson();

        addField(sb, STREAM_NAME, streamName);
        JsonUtils.addField(sb, ACTION, action.actionText);
        JsonUtils.addField(sb, CONFIG, config);

        return endJson(sb).toString();
    }

    @Override
    public String toString() {
        return "ConsumerCreateRequest{" +
                "streamName='" + streamName + '\'' +
                ", " + config +
                '}';
    }
}
