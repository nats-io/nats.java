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
import java.util.List;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.objectString;

/**
 * The StreamInfo class contains information about a JetStream stream.
 */
public class StreamInfo extends ApiResponse<StreamInfo> {

    private final ZonedDateTime created;
    private final StreamConfiguration config;
    private final StreamState state;
    private final ClusterInfo clusterInfo;
    private final MirrorInfo mirrorInfo;
    private final List<SourceInfo> sourceInfos;

    public StreamInfo(Message msg) {
        this(new String(msg.getData(), StandardCharsets.UTF_8));
    }

    public StreamInfo(String json) {
        super(json);
        created = JsonUtils.readDate(json, CREATED_RE);
        config = StreamConfiguration.instance(JsonUtils.getJsonObject(CONFIG, json));
        state = new StreamState(JsonUtils.getJsonObject(STATE, json));
        clusterInfo = ClusterInfo.optionalInstance(json);
        mirrorInfo = MirrorInfo.optionalInstance(json);
        sourceInfos = SourceInfo.optionalListOf(json);
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

    public MirrorInfo getMirrorInfo() {
        return mirrorInfo;
    }

    public List<SourceInfo> getSourceInfos() {
        return sourceInfos;
    }

    public ClusterInfo getClusterInfo() {
        return clusterInfo;
    }

    @Override
    public String toString() {
        return "StreamInfo{" +
                "created=" + created +
                ", " + objectString("config", config) +
                ", " + objectString("state", state) +
                ", " + objectString("cluster", clusterInfo) +
                ", " + objectString("mirror", mirrorInfo) +
                ", sources=" + sourceInfos +
                '}';
    }
}
