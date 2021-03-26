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

import java.util.List;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.*;

/**
 * Placement directives to consider when placing replicas of a stream
 */
public class Placement implements JsonSerializable {
    private final String cluster;
    private final List<String> tags;

    static Placement optionalInstance(String fullJson) {
        String objJson = JsonUtils.getJsonObject(PLACEMENT, fullJson, null);
        return objJson == null ? null : new Placement(objJson);
    }

    Placement(String json) {
        cluster = JsonUtils.readString(json, CLUSTER_RE);
        tags = JsonUtils.getStringList(TAGS, json);
    }

    /**
     * The desired cluster name to place the stream.
     * @return The cluster name
     */
    public String getCluster() {
        return cluster;
    }

    /**
     * Tags required on servers hosting this stream
     * @return the list of tags
     */
    public List<String> getTags() {
        return tags;
    }

    @Override
    public String toString() {
        return "Placement{" +
                "cluster='" + cluster + '\'' +
                ", tags=" + tags +
                '}';
    }

    public String toJson() {
        StringBuilder sb = beginJson();
        addField(sb, CLUSTER, cluster);
        addStrings(sb, TAGS, tags);
        return endJson(sb).toString();
    }
}
