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

import io.nats.client.support.JsonUtils;

import java.util.List;

import static io.nats.client.support.ApiConstants.*;

/**
 * Information about the cluster a stream is part of.
 */
public class ClusterInfo {
    private final String name;
    private final String leader;
    private final List<Replica> replicas;

    static ClusterInfo optionalInstance(String fullJson) {
        String objJson = JsonUtils.getJsonObject(CLUSTER, fullJson, null);
        return objJson == null ? null : new ClusterInfo(objJson);
    }

    ClusterInfo(String json) {
        name = JsonUtils.readString(json, NAME_RE);
        leader = JsonUtils.readString(json, LEADER_RE);
        replicas = Replica.optionalListOf(json);
    }

    public String getName() {
        return name;
    }

    public String getLeader() {
        return leader;
    }

    public List<Replica> getReplicas() {
        return replicas;
    }

    @Override
    public String toString() {
        return "ClusterInfo{" +
                "name='" + name + '\'' +
                ", leader='" + leader + '\'' +
                ", replicas=" + replicas +
                '}';
    }
}
