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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static io.nats.client.support.ApiConstants.NAME;
import static io.nats.client.support.JsonUtils.*;

/**
 * TBD
 */
public class EndpointStats implements JsonSerializable {
    public final String name;
    public AtomicLong numRequests;
    public AtomicLong numErrors;
    public String lastError;
    public AtomicLong totalProcessingTime;
    public AtomicLong averageProcessingTime;

    public EndpointStats(String name) {
        this.name = name;
        this.numRequests = new AtomicLong();
        this.numErrors = new AtomicLong();
        this.lastError = null;
        this.totalProcessingTime = new AtomicLong();
        this.averageProcessingTime = new AtomicLong();
    }

    public void reset() {
        numRequests.set(0);
        numErrors.set(0);
        lastError = null;
        totalProcessingTime.set(0);
        averageProcessingTime.set(0);
    }

    static List<EndpointStats> optionalListOf(String json) {
        List<String> listJson = getObjectList("stats", json);
        List<EndpointStats> list = new ArrayList<>();
        for (String j : listJson) {
            list.add(fromJson(j));
        }
        return list.isEmpty() ? null : list;
    }

    static EndpointStats fromJson(String json) {
        EndpointStats endpointStats =
            new EndpointStats(JsonUtils.readString(json, string_pattern(NAME)));
        endpointStats.numRequests.set(JsonUtils.readLong(json, integer_pattern("num_requests"), 0));
        endpointStats.numErrors.set(JsonUtils.readLong(json, integer_pattern("num_errors"), 0));
        endpointStats.lastError = JsonUtils.readString(json, string_pattern("last_error"));
        endpointStats.totalProcessingTime.set(JsonUtils.readLong(json, integer_pattern("total_processing_time"), 0));
        endpointStats.averageProcessingTime.set(JsonUtils.readLong(json, integer_pattern("average_processing_time"), 0));
        return endpointStats;
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, NAME, name);
        JsonUtils.addField(sb, "num_requests", numRequests.get());
        JsonUtils.addField(sb, "num_errors", numErrors.get());
        JsonUtils.addField(sb, "last_error", lastError);
        JsonUtils.addField(sb, "total_processing_time", totalProcessingTime.get());
        JsonUtils.addField(sb, "average_processing_time", averageProcessingTime.get());
        return endJson(sb).toString();
    }
}
