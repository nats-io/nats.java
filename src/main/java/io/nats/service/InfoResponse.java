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

import io.nats.client.support.JsonUtils;
import io.nats.client.support.JsonValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.nats.client.support.ApiConstants.DESCRIPTION;
import static io.nats.client.support.ApiConstants.ENDPOINTS;
import static io.nats.client.support.JsonUtils.listEquals;
import static io.nats.client.support.JsonValueUtils.*;

/**
 * Info response class forms the info json payload, for example:
 * <code>{"id":"JlkwZvmHAXCQGwwxiPwaBJ","name":"MyService","version":"0.0.1","endpoints":[{"name":"MyEndpoint","subject":"myend"}],"type":"io.nats.micro.v1.info_response"}</code>
 */
public class InfoResponse extends ServiceResponse {
    public static final String TYPE = "io.nats.micro.v1.info_response";

    private final String description;
    private final List<Endpoint> endpoints;

    InfoResponse(String id, String name, String version, Map<String, String> metadata, String description) {
        super(TYPE, id, name, version, metadata);
        this.description = description;
        this.endpoints = new ArrayList<>();
    }

    InfoResponse addServiceEndpoint(ServiceEndpoint se) {
        endpoints.add(new Endpoint(
            se.getName(),
            se.getSubject(),
            se.getQueueGroup(),
            se.getMetadata()
        ));
        serialized.set(null);
        return this;
    }

    InfoResponse(byte[] jsonBytes) {
        this(parseMessage(jsonBytes));
    }

    private InfoResponse(JsonValue jv) {
        super(TYPE, jv);
        description = readString(jv, DESCRIPTION);
        endpoints = read(jv, ENDPOINTS, v -> listOf(v, Endpoint::new));

    }

    @Override
    protected void subToJson(StringBuilder sb) {
        JsonUtils.addField(sb, DESCRIPTION, description);
        JsonUtils.addJsons(sb, ENDPOINTS, endpoints, true);
    }

    /**
     * Description for the service
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * List of endpoints
     * @return the endpoints
     */
    public List<Endpoint> getEndpoints() {
        return endpoints;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        InfoResponse that = (InfoResponse) o;

        if (!Objects.equals(description, that.description)) return false;
        return listEquals(endpoints, that.endpoints);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (endpoints != null ? endpoints.hashCode() : 0);
        return result;
    }
}
