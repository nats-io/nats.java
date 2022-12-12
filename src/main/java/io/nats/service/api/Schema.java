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

import static io.nats.client.support.JsonUtils.*;

/**
 * TBD
 */
public class Schema implements JsonSerializable {
    private final String request;
    private final String response;

    static Schema optionalInstance(String fullJson) {
        String objJson = JsonUtils.getJsonObject("schema", fullJson, null);
        return objJson == null ? null : new Schema(objJson);
    }

    public Schema(String request, String response) {
        this.request = request;
        this.response = response;
    }

    public Schema(ServiceDescriptor descriptor) {
        this.request = descriptor.schemaRequest;
        this.response = descriptor.schemaResponse;
    }

    protected Schema(String json) {
        request = JsonUtils.readString(json, string_pattern("request"));
        response = JsonUtils.readString(json, string_pattern("response"));
    }

    @Override
    public String toJson() {
        return endJson(appendSuperFields(beginJson())).toString();
    }

    protected StringBuilder appendSuperFields(StringBuilder sb) {
        JsonUtils.addField(sb, "request", request);
        JsonUtils.addField(sb, "response", response);
        return sb;
    }

    /**
     * A string or url
     * @return the request info
     */
    public String getRequest() {
        return request;
    }

    /**
     * A string or url
     * @return the response info
     */
    public String getResponse() {
        return response;
    }

    @Override
    public String toString() {
        return toJson();
    }
}
