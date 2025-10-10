// Copyright 2022 The NATS Authors
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
import io.nats.client.support.JsonValue;
import io.nats.client.support.Validator;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.nats.client.support.ApiConstants.CLUSTER;
import static io.nats.client.support.ApiConstants.TAGS;
import static io.nats.client.support.JsonUtils.*;
import static io.nats.client.support.JsonValueUtils.readOptionalStringList;
import static io.nats.client.support.JsonValueUtils.readStringEmptyAsNull;

/**
 * Placement directives to consider when placing replicas of a stream
 */
public class Placement implements JsonSerializable {
    private final String cluster;
    private final List<String> tags;

    static Placement optionalInstance(JsonValue vPlacement) {
        return vPlacement == null ? null : new Placement(vPlacement);
    }

    Placement(JsonValue vPlacement) {
        this.cluster = readStringEmptyAsNull(vPlacement, CLUSTER);
        this.tags = readOptionalStringList(vPlacement, TAGS);
    }

    /**
     * Construct a placement object
     * @param cluster the cluster name
     * @param tags the list of tags, may be null
     */
    public Placement(String cluster, List<String> tags) {
        this.cluster = cluster == null || cluster.isEmpty() ? null : cluster;
        this.tags = tags == null || tags.isEmpty() ? null : tags;
    }

    public boolean hasData() {
        return cluster != null || tags != null;
    }

    /**
     * The desired cluster name to place the stream.
     * @return The cluster name
     */
    @Nullable
    public String getCluster() {
        return cluster;
    }

    /**
     * Tags required on servers hosting this stream
     * @return the list of tags
     */
    @Nullable
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

    @Override
    @NonNull
    public String toJson() {
        StringBuilder sb = beginJson();
        addField(sb, CLUSTER, cluster);
        addStrings(sb, TAGS, tags);
        return endJson(sb).toString();
    }

    /**
     * Creates a builder for a placements object.
     * @return the builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Placement can be created using a Builder.
     */
    public static class Builder {
        private String cluster;
        private List<String> tags;

        /**
         * Set the cluster string.
         * @param cluster the cluster
         * @return the builder
         */
        public Builder cluster(String cluster) {
            this.cluster = cluster;
            return this;
        }

        /**
         * Set the tags
         * @param tags the list of tags
         * @return the builder
         */
        public Builder tags(String... tags) {
            if (tags == null || tags.length == 0) {
                this.tags = null;
                return this;
            }
            return tags(Arrays.asList(tags));
        }

        /**
         * Set the tags
         * @param tags the list of tags
         * @return the builder
         */
        public Builder tags(List<String> tags) {
            if (tags == null || tags.size() == 0) {
                this.tags = null;
                return this;
            }
            this.tags = new ArrayList<>();
            for (String tag : tags) {
                if (!Validator.nullOrEmpty(tag)) {
                    this.tags.add(tag);
                }
            }
            if (this.tags.size() == 0) {
                this.tags = null;
            }
            return this;
        }

        /**
         * Build a Placement object
         * @return the Placement
         */
        public Placement build() {
            return new Placement(cluster, tags);
        }
    }
}
