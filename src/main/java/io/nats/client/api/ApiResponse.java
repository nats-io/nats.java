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
import io.nats.client.support.*;

import static io.nats.client.support.ApiConstants.ERROR;
import static io.nats.client.support.ApiConstants.TYPE;
import static io.nats.client.support.JsonValueUtils.readString;
import static io.nats.client.support.JsonValueUtils.readValue;

public abstract class ApiResponse<T> {

    public static final String NO_TYPE = "io.nats.jetstream.api.v1.no_type";
    public static final String PARSE_ERROR_TYPE = "io.nats.client.api.parse_error";

    protected final JsonValue jv;

    private final String type;
    private final Error error;

    public ApiResponse(Message msg) {
        this(parseMessage(msg));
    }

    protected static JsonValue parseMessage(Message msg) {
        if (msg == null) {
            return null;
        }
        try {
            return JsonParser.parse(msg.getData());
        }
        catch (JsonParseException e) {
            return JsonValueUtils.mapBuilder()
                .put(ERROR, new Error(500, "Error parsing: " + e.getMessage()))
                .put(TYPE, PARSE_ERROR_TYPE)
                .toJsonValue();
        }
    }

    public ApiResponse(JsonValue jsonValue) {
        jv = jsonValue;
        if (jv == null) {
            error = null;
            type = null;
        }
        else {
            error = Error.optionalInstance(readValue(jv, ERROR));
            String temp = readString(jv, TYPE);
            if (temp == null) {
                type = NO_TYPE;
            }
            else {
                type = temp;
                jv.map.remove(TYPE); // just so it's not in the toString, it's very long and the object name will be there
            }
        }
    }

    public ApiResponse() {
        jv = null;
        error = null;
        type = NO_TYPE;
    }

    public ApiResponse(Error error) {
        jv = null;
        this.error = error;
        type = NO_TYPE;
    }

    @SuppressWarnings("unchecked")
    public T throwOnHasError() throws JetStreamApiException {
        if (hasError()) {
            throw new JetStreamApiException(this);
        }
        return (T)this;
    }

    public JsonValue getJv() {
        return jv;
    }

    public boolean hasError() {
        return error != null;
    }

    public String getType() {
        return type;
    }

    public int getErrorCode() {
        return error == null ? Error.NOT_SET : error.getCode();
    }

    public int getApiErrorCode() {
        return error == null ? Error.NOT_SET : error.getApiErrorCode();
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

    @Override
    public String toString() {
        return jv == null
            ? JsonUtils.toKey(getClass()) + "\":null"
            : jv.toString(getClass());
    }
}
