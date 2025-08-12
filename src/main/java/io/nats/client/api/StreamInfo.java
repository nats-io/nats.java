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
import io.nats.client.support.DateTimeUtils;
import io.nats.client.support.JsonValue;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.ZonedDateTime;
import java.util.List;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonParser.parseUnchecked;
import static io.nats.client.support.JsonValueUtils.readDate;
import static io.nats.client.support.JsonValueUtils.readValue;
import static io.nats.client.support.NatsConstants.UNDEFINED;

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
    private final List<StreamAlternate> alternates;
    private final ZonedDateTime timestamp;

    public StreamInfo(@NonNull Message msg) {
        this(parseUnchecked(msg.getData()));
    }

    public StreamInfo(@NonNull JsonValue vStreamInfo) {
        super(vStreamInfo);
        if (hasError()) {
            createTime = DateTimeUtils.DEFAULT_TIME;
            config = StreamConfiguration.builder().name(UNDEFINED).build();
            streamState = new StreamState(JsonValue.EMPTY_MAP);
            clusterInfo = null;
            mirrorInfo = null;
            sourceInfos = null;
            alternates = null;
            timestamp = null;
        }
        else {
            JsonValue jvConfig = nullValueIsError(jv, CONFIG, JsonValue.NULL);
            config = (jvConfig == JsonValue.NULL)
                ? StreamConfiguration.builder().name(UNDEFINED).build()
                : StreamConfiguration.instance(jvConfig);

            createTime = nullDateIsError(jv, CREATED);

            streamState = new StreamState(readValue(jv, STATE));
            clusterInfo = ClusterInfo.optionalInstance(readValue(jv, CLUSTER));
            mirrorInfo = MirrorInfo.optionalInstance(readValue(jv, MIRROR));
            sourceInfos = SourceInfo.optionalListOf(readValue(jv, SOURCES));
            alternates = StreamAlternate.optionalListOf(readValue(jv, ALTERNATES));
            timestamp = readDate(jv, TIMESTAMP);
        }
    }

    /**
     * Gets the stream configuration. Same as getConfig
     * @return the stream configuration.
     */
    @NonNull
    public StreamConfiguration getConfiguration() {
        return config;
    }

    /**
     * Gets the stream configuration. Same as getConfiguration
     * @return the stream configuration.
     */
    @NonNull
    public StreamConfiguration getConfig() {
        return config;
    }

    /**
     * Gets the stream state.
     * @return the stream state
     */
    @NonNull
    public StreamState getStreamState() {
        return streamState;
    }

    /**
     * Gets the creation time of the stream.
     * @return the creation date and time.
     */
    @NonNull
    public ZonedDateTime getCreateTime() {
        return createTime;
    }

    @Nullable
    public MirrorInfo getMirrorInfo() {
        return mirrorInfo;
    }

    @Nullable
    public List<SourceInfo> getSourceInfos() {
        return sourceInfos;
    }

    @Nullable
    public ClusterInfo getClusterInfo() {
        return clusterInfo;
    }

    @Nullable
    public List<StreamAlternate> getAlternates() {
        return alternates;
    }

    /**
     * Gets the server time the info was gathered
     * @return the server gathered timed
     */
    @Nullable // doesn't exist in some versions of the server
    public ZonedDateTime getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "StreamInfo " + jv;
    }
}
