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

import static io.nats.client.support.ApiConstants.NAME;
import static io.nats.client.support.JsonUtils.*;

/**
 * TBD
 */
public class PingResponse implements JsonSerializable {
    private final String serviceId;
    private final String name;

    public PingResponse(String serviceId, String name) {
        this.serviceId = serviceId;
        this.name = name;
    }

    public PingResponse(String json) {
        name = JsonUtils.readString(json, string_pattern(NAME));
        serviceId = JsonUtils.readString(json, string_pattern("id"));
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, NAME, name);
        JsonUtils.addField(sb, "id", serviceId);
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

    @Override
    public String toString() {
        return toJson();
    }
}
