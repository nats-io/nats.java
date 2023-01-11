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

import io.nats.client.support.JsonValue;

import java.util.List;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonValueUtils.getMappedString;
import static io.nats.client.support.JsonValueUtils.getMappedValue;

/**
 * Information about the cluster a stream is part of.
 */
public class ClusterInfo {
    private final String name;
    private final String leader;
    private final List<Replica> replicas;

    static ClusterInfo optionalInstance(JsonValue v) {
        return v == null ? null : new ClusterInfo(v);
    }

    ClusterInfo(JsonValue v) {
        name = getMappedString(v, NAME);
        leader = getMappedString(v, LEADER);
        replicas = Replica.optionalListOf(getMappedValue(v, REPLICAS));
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
