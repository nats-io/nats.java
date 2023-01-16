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
import io.nats.client.support.Validator;

import static io.nats.client.support.ApiConstants.REQUEST;
import static io.nats.client.support.ApiConstants.RESPONSE;
import static io.nats.client.support.JsonUtils.endJson;
import static io.nats.client.support.JsonValueUtils.readString;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class Schema implements JsonSerializable {
    private final String request;
    private final String response;

    public static Schema optionalInstance(JsonValue vSchema) {
        return vSchema == null ? null : new Schema(vSchema);
    }

    public static Schema optionalInstance(String request, String response) {
        request = Validator.emptyAsNull(request);
        response = Validator.emptyAsNull(response);
        return request == null && response == null ? null : new Schema(request, response);
    }

    public Schema(String request, String response) {
        this.request = request;
        this.response = response;
    }

    protected Schema(JsonValue vSchema) {
        request = readString(vSchema, REQUEST);
        response = readString(vSchema, RESPONSE);
    }

    @Override
    public String toJson() {
        StringBuilder sb = JsonUtils.beginJson();
        JsonUtils.addField(sb, REQUEST, request);
        JsonUtils.addField(sb, RESPONSE, response);
        return endJson(sb).toString();
    }

    @Override
    public String toString() {
        return toJson();
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
}
