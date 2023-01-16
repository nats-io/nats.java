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
import io.nats.client.support.JsonValue;
import io.nats.client.support.JsonValueUtils;

import java.time.ZonedDateTime;
import java.util.List;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;
import static io.nats.client.support.JsonValueUtils.*;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class EndpointStats implements JsonSerializable {
    private final String name;
    private final String subject;
    private final long numRequests;
    private final long numErrors;
    private final long processingTime;
    private final long averageProcessingTime;
    private final String lastError;
    private final JsonValue data;
    private final ZonedDateTime started; // THIS FIELD IS CURRENTLY IN JAVA ONLY

    static List<EndpointStats> listOf(JsonValue vEndpointStats) {
        return JsonValueUtils.listOf(vEndpointStats, EndpointStats::new);
    }

    public EndpointStats(String name, String subject, long numRequests, long numErrors, long processingTime, long averageProcessingTime, String lastError, JsonValue data, ZonedDateTime started) {
        this.name = name;
        this.subject = subject;
        this.numRequests = numRequests;
        this.numErrors = numErrors;
        this.processingTime = processingTime;
        this.averageProcessingTime = averageProcessingTime;
        this.lastError = lastError;
        this.data = data;
        this.started = started;
    }

    public EndpointStats(JsonValue vEndpointStats) {
        name = readString(vEndpointStats, NAME);
        subject = readString(vEndpointStats, SUBJECT);
        numRequests = readLong(vEndpointStats, NUM_REQUESTS, 0);
        numErrors = readLong(vEndpointStats, NUM_ERRORS, 0);
        processingTime = readLong(vEndpointStats, PROCESSING_TIME, 0);
        averageProcessingTime = readLong(vEndpointStats, AVERAGE_PROCESSING_TIME, 0);
        lastError = readString(vEndpointStats, LAST_ERROR);
        data = readValue(vEndpointStats, DATA);
        started = readDate(vEndpointStats, STARTED);
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, NAME, name);
        JsonUtils.addField(sb, SUBJECT, subject);
        JsonUtils.addField(sb, NUM_REQUESTS, numRequests);
        JsonUtils.addField(sb, NUM_ERRORS, numErrors);
        JsonUtils.addField(sb, PROCESSING_TIME, processingTime);
        JsonUtils.addField(sb, AVERAGE_PROCESSING_TIME, averageProcessingTime);
        JsonUtils.addField(sb, LAST_ERROR, lastError);
        JsonUtils.addField(sb, DATA, data);
        JsonUtils.addField(sb, STARTED, started);
        return endJson(sb).toString();
    }

    public String getName() {
        return name;
    }

    public String getSubject() {
        return subject;
    }

    public long getNumRequests() {
        return numRequests;
    }

    public long getNumErrors() {
        return numErrors;
    }

    public long getProcessingTime() {
        return processingTime;
    }

    public long getAverageProcessingTime() {
        return averageProcessingTime;
    }

    public String getLastError() {
        return lastError;
    }

    public JsonValue getData() {
        return data;
    }

    @Override
    public String toString() {
        return toJson();
    }
}
