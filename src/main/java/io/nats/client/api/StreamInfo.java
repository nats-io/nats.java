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

import java.time.ZonedDateTime;
import java.util.List;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonParser.parseUnchecked;
import static io.nats.client.support.JsonValueUtils.readDate;
import static io.nats.client.support.JsonValueUtils.readValue;

/**
 * The StreamInfo class contains information about a JetStream stream.
 */
public class StreamInfo extends ApiResponse<StreamInfo> {

    private final ZonedDateTime createTime;
    private final StreamConfiguration config;
    private final StreamState streamState;
    private final ClusterInfo clusterInfo;
    private final MirrorInfo mirrorInfo;
    private final List<SourceInfo> sourceInfos;
    private final ZonedDateTime timestamp;

    public StreamInfo(Message msg) {
        this(parseUnchecked(msg.getData()));
    }

    public StreamInfo(JsonValue vStreamInfo) {
        super(vStreamInfo);
        createTime = readDate(jv, CREATED);
        config = StreamConfiguration.instance(readValue(jv, CONFIG));
        streamState = new StreamState(readValue(jv, STATE));
        clusterInfo = ClusterInfo.optionalInstance(readValue(jv, CLUSTER));
        mirrorInfo = MirrorInfo.optionalInstance(readValue(jv, MIRROR));
        sourceInfos = SourceInfo.optionalListOf(readValue(jv, SOURCES));
        timestamp = readDate(jv, TIMESTAMP);
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
        return streamState;
    }

    /**
     * Gets the creation time of the stream.
     * @return the creation date and time.
     */
    public ZonedDateTime getCreateTime() {
        return createTime;
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

    public StreamConfiguration getConfig() {
        return config;
    }

    /**
     * Gets the server time the info was gathered
     * @return the server gathered timed
     */
    public ZonedDateTime getTimestamp() {
        return timestamp;
    }
}
