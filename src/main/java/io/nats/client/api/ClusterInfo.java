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
import org.jetbrains.annotations.Nullable;

import java.util.List;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonValueUtils.readString;
import static io.nats.client.support.JsonValueUtils.readValue;

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
        name = readString(v, NAME);
        leader = readString(v, LEADER);
        replicas = Replica.optionalListOf(readValue(v, REPLICAS));
    }

    /**
     * The cluster name. Technically can be null
     * @return the cluster or null
     */
    @Nullable
    public String getName() {
        return name;
    }

    /**
     * The server name of the RAFT leader
     * @return the leader or null
     */
    @Nullable
    public String getLeader() {
        return leader;
    }

    /**
     * The members of the RAFT cluster. May be null if there are no replicas.
     * @return the replicas or null
     */
    @Nullable
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
