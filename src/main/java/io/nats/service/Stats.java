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

import io.nats.client.support.ApiConstants;
import io.nats.client.support.DateTimeUtils;
import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonUtils;

import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class Stats implements JsonSerializable {
    public static final String TYPE = "io.nats.micro.v1.stats_response";

    private final String serviceId;
    private final String name;
    private final String version;
    private final AtomicLong numRequests;
    private final AtomicLong numErrors;
    private final AtomicReference<String> lastError;
    private final AtomicLong processingTime;
    private final AtomicLong averageProcessingTime;
    private StatsData data;
    private ZonedDateTime started;

    public Stats(String serviceId, String name, String version) {
        this.serviceId = serviceId;
        this.name = name;
        this.version = version;
        this.numRequests = new AtomicLong();
        this.numErrors = new AtomicLong();
        this.lastError = new AtomicReference<>();
        this.processingTime = new AtomicLong();
        this.averageProcessingTime = new AtomicLong();
        started = DateTimeUtils.gmtNow();
    }

    public Stats copy(Function<String, StatsData> decoder) {
        Stats copy = new Stats(serviceId, name, version);
        copy.numRequests.set(numRequests.get());
        copy.numErrors.set(numErrors.get());
        copy.lastError.set(lastError.get());
        copy.processingTime.set(processingTime.get());
        copy.averageProcessingTime.set(averageProcessingTime.get());
        if (data != null && decoder != null) {
            copy.data = decoder.apply(data.toJson());
        }
        copy.started = DateTimeUtils.toGmt(started);
        return copy;
    }

    public Stats(String json, Function<String, StatsData> decoder) {
        // handle the data first just in the off chance that the data has a duplicate
        // field name to the stats. This is because we don't have a proper parse, but it works fine.
        String dataJson = JsonUtils.getJsonObject(DATA, json, null);
        if (dataJson != null) {
            if (decoder != null) {
                data = decoder.apply(dataJson);
            }
            JsonUtils.removeObject(json, DATA);
        }

        name = JsonUtils.readString(json, NAME_RE);
        serviceId = JsonUtils.readString(json, ID_RE);
        version = JsonUtils.readString(json, VERSION_RE);
        numRequests = new AtomicLong(JsonUtils.readLong(json, NUM_REQUESTS_RE, 0));
        numErrors = new AtomicLong(JsonUtils.readLong(json, NUM_ERRORS_RE, 0));
        lastError = new AtomicReference<>(JsonUtils.readString(json, LAST_ERROR_RE));
        processingTime = new AtomicLong(JsonUtils.readLong(json, PROCESSING_TIME_RE, 0));
        averageProcessingTime = new AtomicLong(JsonUtils.readLong(json, AVERAGE_PROCESSING_TIME_RE, 0));
        started = JsonUtils.readDate(json, STARTED_RE);
    }

    public void reset() {
        numRequests.set(0);
        numErrors.set(0);
        lastError.set(null);
        processingTime.set(0);
        averageProcessingTime.set(0);
        data = null;
        started = DateTimeUtils.gmtNow();
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, NAME, name);
        JsonUtils.addField(sb, ApiConstants.TYPE, TYPE);
        JsonUtils.addField(sb, ID, serviceId);
        JsonUtils.addField(sb, VERSION, version);
        JsonUtils.addField(sb, NUM_REQUESTS, numRequests.get());
        JsonUtils.addField(sb, NUM_ERRORS, numErrors.get());
        JsonUtils.addField(sb, LAST_ERROR, lastError.get());
        JsonUtils.addField(sb, PROCESSING_TIME, processingTime.get());
        JsonUtils.addField(sb, AVERAGE_PROCESSING_TIME, averageProcessingTime.get());
        if (data != null) {
            JsonUtils.addRawJson(sb, DATA, data.toJson());
        }
        JsonUtils.addField(sb, STARTED, started);
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
     * The type of this. Always {@value #TYPE}
     * @return the type string
     */
    public String getType() {
        return TYPE;
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

    public long getNumRequests() {
        return numRequests.get();
    }

    public long getNumErrors() {
        return numErrors.get();
    }

    public String getLastError() {
        return lastError.get();
    }

    public long getProcessingTime() {
        return processingTime.get();
    }

    public long getAverageProcessingTime() {
        return averageProcessingTime.get();
    }

    public StatsData getData() {
        return data;
    }

    public ZonedDateTime getStarted() {
        return started;
    }

    public long incrementNumRequests() {
        return this.numRequests.incrementAndGet();
    }

    public void incrementNumErrors() {
        this.numErrors.incrementAndGet();
    }

    public void setLastError(String lastError) {
        this.lastError.set(lastError);
    }

    public long addTotalProcessingTime(long elapsed) {
        return this.processingTime.addAndGet(elapsed);
    }

    public void setAverageProcessingTime(long averageProcessingTime) {
        this.averageProcessingTime.set(averageProcessingTime);
    }

    public void setData(StatsData data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return toJson();
    }
}
