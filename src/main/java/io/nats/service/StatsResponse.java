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

package io.nats.service;

import io.nats.client.support.JsonUtils;
import io.nats.client.support.JsonValue;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;

import static io.nats.client.support.ApiConstants.ENDPOINTS;
import static io.nats.client.support.ApiConstants.STARTED;
import static io.nats.client.support.JsonValueUtils.readDate;
import static io.nats.client.support.JsonValueUtils.readValue;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class StatsResponse extends ServiceResponse {
    public static final String TYPE = "io.nats.micro.v1.stats_response";

    private final ZonedDateTime started;
    private final List<EndpointStats> endpointStats;

    public StatsResponse(ServiceResponse template, ZonedDateTime started, List<EndpointStats> endpointStats) {
        super(TYPE, template);
        this.started = started;
        this.endpointStats = endpointStats;
    }

    public StatsResponse(byte[] jsonBytes) {
        this(parseMessage(jsonBytes));
    }

    private StatsResponse(JsonValue jv) {
        super(TYPE, jv);
        endpointStats = EndpointStats.listOf(readValue(jv, ENDPOINTS));
        started = readDate(jv, STARTED);
    }

    @Override
    protected void subToJson(StringBuilder sb, boolean forToString) {
        JsonUtils.addJsons(sb, ENDPOINTS, endpointStats);
        JsonUtils.addField(sb, STARTED, started);
    }

    public ZonedDateTime getStarted() {
        return started;
    }

    public List<EndpointStats> getEndpointStats() {
        return endpointStats;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        StatsResponse that = (StatsResponse) o;

        if (!Objects.equals(started, that.started)) return false;
        return Objects.equals(endpointStats, that.endpointStats);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (started != null ? started.hashCode() : 0);
        result = 31 * result + (endpointStats != null ? endpointStats.hashCode() : 0);
        return result;
    }
}
