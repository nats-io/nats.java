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

package io.nats.service.api;

import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonUtils;
import io.nats.service.ServiceDescriptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.*;
import static io.nats.service.ServiceUtil.ENDPOINT_STATS_COMPARATOR;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class StatsResponse implements JsonSerializable {
    private final String serviceId;
    private final String name;
    private final String version;
    private final List<EndpointStats> stats;

    public StatsResponse(String serviceId, ServiceDescriptor descriptor, EndpointStats stats) {
        this.serviceId = serviceId;
        this.name = descriptor.name;
        this.version = descriptor.version;
        this.stats = Collections.singletonList(stats);
    }

    public StatsResponse(String serviceId, ServiceDescriptor descriptor, Collection<EndpointStats> stats) {
        this.serviceId = serviceId;
        this.name = descriptor.name;
        this.version = descriptor.version;
        this.stats = new ArrayList<>(stats);
        this.stats.sort(ENDPOINT_STATS_COMPARATOR);
    }

    public StatsResponse(String serviceId, String name, String version, EndpointStats stats) {
        this.serviceId = serviceId;
        this.name = name;
        this.version = version;
        this.stats = Collections.singletonList(stats);
    }

    public StatsResponse(String serviceId, String name, String version, Collection<EndpointStats> stats) {
        this.name = name;
        this.serviceId = serviceId;
        this.version = version;
        this.stats = new ArrayList<>(stats);
        this.stats.sort(ENDPOINT_STATS_COMPARATOR);
    }

    public StatsResponse(String json) {
        name = JsonUtils.readString(json, NAME_RE);
        serviceId = JsonUtils.readString(json, ID_RE);
        version = JsonUtils.readString(json, VERSION_RE);
        stats = EndpointStats.toList(json);
        this.stats.sort(ENDPOINT_STATS_COMPARATOR);
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, NAME, name);
        JsonUtils.addField(sb, ID, serviceId);
        JsonUtils.addField(sb, VERSION, version);
        addJsons(sb, STATS, stats);
        return endJson(sb).toString();
    }

    /**
     * The kind of the service reporting the status
     * @return the service name
     */
    public String getName() {
        return name;
    }

    /**
     * The unique ID of the service reporting the status
     * @return the service id
     */
    public String getServiceId() {
        return serviceId;
    }

    /**
     * Version of the service
     * @return the version
     */
    public String getVersion() {
        return version;
    }

    /**
     *
     * @return
     */
    public List<EndpointStats> getStats() {
        return stats;
    }

    @Override
    public String toString() {
        return toJson();
    }
}
