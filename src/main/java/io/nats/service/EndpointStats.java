// Copyright 2023 The NATS Authors
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

import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonUtils;
import io.nats.client.support.JsonValue;
import io.nats.client.support.JsonValueUtils;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;
import static io.nats.client.support.JsonValueUtils.*;

/**
 * Endpoints stats contains various stats and custom data for an endpoint.
 * <code>
 * {
 * "id": "ZP1oVevzLGu4CBORMXKKke",
 * "name": "Service1",
 * "version": "0.0.1",
 * "endpoints": [{
 *     "name": "SortEndpointAscending",
 *     "subject": "sort.ascending",
 *     "queue_group": "q",
 *     "num_requests": 1,
 *     "processing_time": 538900,
 *     "average_processing_time": 538900,
 *     "started": "2023-08-15T13:51:41.318000000Z"
 * }
 * </code>
 * <code>
 * {
 *     "name": "SortEndpointDescending",
 *     "subject": "sort.descending",
 *     "num_requests": 1,
 *     "processing_time": 88400,
 *     "average_processing_time": 88400,
 *     "started": "2023-08-15T13:51:41.318000000Z"
 * }
 * </code>
 * <code>
 * {
 *     "name": "EchoEndpoint",
 *     "subject": "echo",
 *     "num_requests": 5,
 *     "processing_time": 1931600,
 *     "average_processing_time": 386320,
 *     "data": {
 *          "idata": 2,
 *          "sdata": "s-996409223"
 *     },
 *     "started": "2023-08-15T13:51:41.318000000Z"
 * }
 * </code>
 */
public class EndpointStats implements JsonSerializable {
    private final String name;
    private final String subject;
    private final String queueGroup;
    private final long numRequests;
    private final long numErrors;
    private final long processingTime;
    private final long averageProcessingTime;
    private final String lastError;
    private final JsonValue data;
    private final ZonedDateTime started;

    static List<EndpointStats> listOf(JsonValue vEndpointStats) {
        return JsonValueUtils.listOf(vEndpointStats, EndpointStats::new);
    }

    EndpointStats(String name, String subject, String queueGroup, long numRequests, long numErrors, long processingTime, String lastError, JsonValue data, ZonedDateTime started) {
        this.name = name;
        this.subject = subject;
        this.queueGroup = queueGroup;
        this.numRequests = numRequests;
        this.numErrors = numErrors;
        this.processingTime = processingTime;
        this.averageProcessingTime = numRequests < 1 ? 0 : processingTime / numRequests;
        this.lastError = lastError;
        this.data = data;
        this.started = started;
    }

    EndpointStats(JsonValue vEndpointStats) {
        name = readString(vEndpointStats, NAME);
        subject = readString(vEndpointStats, SUBJECT);
        queueGroup = readString(vEndpointStats, QUEUE_GROUP);
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
        JsonUtils.addField(sb, QUEUE_GROUP, queueGroup);
        JsonUtils.addFieldWhenGtZero(sb, NUM_REQUESTS, numRequests);
        JsonUtils.addFieldWhenGtZero(sb, NUM_ERRORS, numErrors);
        JsonUtils.addFieldWhenGtZero(sb, PROCESSING_TIME, processingTime);
        JsonUtils.addFieldWhenGtZero(sb, AVERAGE_PROCESSING_TIME, averageProcessingTime);
        JsonUtils.addField(sb, LAST_ERROR, lastError);
        JsonUtils.addField(sb, DATA, data);
        JsonUtils.addField(sb, STARTED, started);
        return endJson(sb).toString();
    }

    /**
     * Get the name of the Endpoint
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Get the subject of the Endpoint
     * @return the subject
     */
    public String getSubject() {
        return subject;
    }

    /**
     * Get the queueGroup of the Endpoint
     * @return the queueGroup
     */
    public String getQueueGroup() {
        return queueGroup;
    }

    /**
     * The number of requests received by the endpoint
     * @return the number of requests
     */
    public long getNumRequests() {
        return numRequests;
    }

    /**
     * Number of errors that the endpoint has raised
     * @return the number of errors
     */
    public long getNumErrors() {
        return numErrors;
    }

    /**
     * Total processing time for the endpoint
     * @return the total processing time
     */
    public long getProcessingTime() {
        return processingTime;
    }

    /**
     * Average processing time is the total processing time divided by the num requests
     * @return the average processing time
     */
    public long getAverageProcessingTime() {
        return averageProcessingTime;
    }

    /**
     * If set, the last error triggered by the endpoint
     * @return the last error or null
     */
    public String getLastError() {
        return lastError;
    }

    /**
     * A field that can be customized with any data as returned by stats handler
     * @return the JsonValue object representing the data
     */
    public JsonValue getData() {
        return data;
    }

    /**
     * The json representation of the custom data. May be null
     * @return the json
     */
    public String getDataAsJson() {
        return data == null ? null : data.toJson();
    }

    /**
     * Get the time the endpoint was started (or restarted)
     * @return the start time
     */
    public ZonedDateTime getStarted() {
        return started;
    }

    @Override
    public String toString() {
        return JsonUtils.toKey(getClass()) + toJson();
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EndpointStats that = (EndpointStats) o;

        if (numRequests != that.numRequests) return false;
        if (numErrors != that.numErrors) return false;
        if (processingTime != that.processingTime) return false;
        if (averageProcessingTime != that.averageProcessingTime) return false;
        if (!Objects.equals(name, that.name)) return false;
        if (!Objects.equals(subject, that.subject)) return false;
        if (!Objects.equals(queueGroup, that.queueGroup)) return false;
        if (!Objects.equals(lastError, that.lastError)) return false;
        if (!Objects.equals(data, that.data)) return false;
        return Objects.equals(started, that.started);
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (subject != null ? subject.hashCode() : 0);
        result = 31 * result + (queueGroup != null ? queueGroup.hashCode() : 0);
        result = 31 * result + (int) (numRequests ^ (numRequests >>> 32));
        result = 31 * result + (int) (numErrors ^ (numErrors >>> 32));
        result = 31 * result + (int) (processingTime ^ (processingTime >>> 32));
        result = 31 * result + (int) (averageProcessingTime ^ (averageProcessingTime >>> 32));
        result = 31 * result + (lastError != null ? lastError.hashCode() : 0);
        result = 31 * result + (data != null ? data.hashCode() : 0);
        result = 31 * result + (started != null ? started.hashCode() : 0);
        return result;
    }
}
