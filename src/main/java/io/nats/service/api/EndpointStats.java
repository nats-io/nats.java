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
import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.*;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class EndpointStats implements JsonSerializable {
    public final String name;
    public AtomicLong numRequests;
    public AtomicLong numErrors;
    public AtomicReference<String> lastError;
    public AtomicLong totalProcessingTime;
    public AtomicLong averageProcessingTime;
    public EndpointStatsData endpointStatsData;

    public EndpointStats(String name) {
        this.name = name;
        this.numRequests = new AtomicLong();
        this.numErrors = new AtomicLong();
        this.lastError = new AtomicReference<>();
        this.totalProcessingTime = new AtomicLong();
        this.averageProcessingTime = new AtomicLong();
        this.endpointStatsData = null; // still have no info for data
    }

    public void reset() {
        numRequests.set(0);
        numErrors.set(0);
        lastError.set(null);
        totalProcessingTime.set(0);
        averageProcessingTime.set(0);
    }

    public static List<EndpointStats> toList(String json) {
        List<String> listJson = getObjectList(STATS, json);
        List<EndpointStats> list = new ArrayList<>();
        for (String j : listJson) {
            list.add(fromJson(j));
        }
        return list;
    }

    static EndpointStats fromJson(String json) {
        EndpointStats endpointStats =
            new EndpointStats(JsonUtils.readString(json, NAME_RE));
        endpointStats.numRequests.set(JsonUtils.readLong(json, NUM_REQUESTS_RE, 0));
        endpointStats.numErrors.set(JsonUtils.readLong(json, NUM_ERRORS_RE, 0));
        endpointStats.lastError.set(JsonUtils.readString(json, LAST_ERROR_RE));
        endpointStats.totalProcessingTime.set(JsonUtils.readLong(json, TOTAL_PROCESSING_TIME_RE, 0));
        endpointStats.averageProcessingTime.set(JsonUtils.readLong(json, AVERAGE_PROCESSING_TIME_RE, 0));
        return endpointStats;
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, NAME, name);
        JsonUtils.addField(sb, NUM_REQUESTS, numRequests.get());
        JsonUtils.addField(sb, NUM_ERRORS, numErrors.get());
        JsonUtils.addField(sb, LAST_ERROR, lastError.get());
        JsonUtils.addField(sb, TOTAL_PROCESSING_TIME, totalProcessingTime.get());
        JsonUtils.addField(sb, AVERAGE_PROCESSING_TIME, averageProcessingTime.get());
        JsonUtils.addField(sb, DATA, endpointStatsData);
        return endJson(sb).toString();
    }

    @Override
    public String toString() {
        return toJson();
    }
}
