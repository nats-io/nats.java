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

import java.util.List;
import java.util.Objects;

import static io.nats.client.support.ApiConstants.API_URL;
import static io.nats.client.support.ApiConstants.ENDPOINTS;
import static io.nats.client.support.JsonValueUtils.readString;
import static io.nats.client.support.JsonValueUtils.readValue;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class SchemaResponse extends ServiceResponse {
    public static final String TYPE = "io.nats.micro.v1.schema_response";

    private final String apiUrl;
    private final List<Endpoint> endpoints;

    public SchemaResponse(String id, String name, String version, String apiUrl, List<Endpoint> endpoints) {
        super(TYPE, id, name, version);
        this.apiUrl = apiUrl;
        this.endpoints = endpoints;
    }

    public SchemaResponse(byte[] jsonBytes) {
        this(parseMessage(jsonBytes));
    }

    private SchemaResponse(JsonValue jv) {
        super(TYPE, jv);
        apiUrl = readString(jv, API_URL);
        endpoints = Endpoint.listOf(readValue(jv, ENDPOINTS));
    }

    @Override
    protected void subToJson(StringBuilder sb, boolean forToString) {
        JsonUtils.addField(sb, API_URL, apiUrl);
        JsonUtils.addJsons(sb, ENDPOINTS, endpoints);
    }

    public String getApiUrl() {
        return apiUrl;
    }

    public List<Endpoint> getEndpoints() {
        return endpoints;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        SchemaResponse that = (SchemaResponse) o;

        if (!Objects.equals(apiUrl, that.apiUrl)) return false;
        return Objects.equals(endpoints, that.endpoints);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (apiUrl != null ? apiUrl.hashCode() : 0);
        result = 31 * result + (endpoints != null ? endpoints.hashCode() : 0);
        return result;
    }
}
