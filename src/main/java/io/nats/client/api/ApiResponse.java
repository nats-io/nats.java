// Copyright 2020 The NATS Authors
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

package io.nats.client.api;

import io.nats.client.JetStreamApiException;
import io.nats.client.Message;
import io.nats.client.support.JsonParser;
import io.nats.client.support.JsonValue;

import static io.nats.client.api.Error.NOT_SET;
import static io.nats.client.support.ApiConstants.ERROR;
import static io.nats.client.support.ApiConstants.TYPE;
import static io.nats.client.support.JsonValueUtils.getMappedString;
import static io.nats.client.support.JsonValueUtils.getMappedValue;
import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class ApiResponse<T> {

    public static final String NO_TYPE = "io.nats.jetstream.api.v1.no_type";

    protected final String json;
    protected final JsonValue jv;

    private final String type;
    private final Error error;

    public ApiResponse(Message msg) {
        this(null, new String(msg.getData(), UTF_8), true);
    }

    public ApiResponse(String json) {
        this(null, json, true);
    }

    public ApiResponse(JsonValue jsonValue) {
        this(jsonValue, null, false);
    }

    private ApiResponse(JsonValue jsonValue, String jsn, boolean saveJson) {
        json = saveJson ? jsn : null;
        jv = jsonValue == null ? (jsn == null ? null : JsonParser.parse(jsn)) : jsonValue;
        error = jv == null ? null : Error.optionalInstance(getMappedValue(jv, ERROR));
        type = jv == null ? NO_TYPE : getMappedString(jv, TYPE, NO_TYPE);
    }

    public ApiResponse() {
        json = null;
        jv = null;
        error = null;
        type = NO_TYPE;
    }

    @SuppressWarnings("unchecked")
    public T throwOnHasError() throws JetStreamApiException {
        if (hasError()) {
            throw new JetStreamApiException(this);
        }
        return (T)this;
    }

    public boolean hasError() {
        return error != null;
    }

    public String getType() {
        return type;
    }

    public int getErrorCode() {
        return error == null ? NOT_SET : error.getCode();
    }

    public int getApiErrorCode() {
        return error == null ? NOT_SET : error.getApiErrorCode();
    }

    public String getDescription() {
        return error == null ? null : error.getDescription();
    }

    public String getError() {
        return error == null ? null : error.toString();
    }

    public Error getErrorObject() {
        return error;
    }
}
