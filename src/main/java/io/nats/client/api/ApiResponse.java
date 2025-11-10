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
import org.jspecify.annotations.Nullable;

import java.time.ZonedDateTime;

import static io.nats.client.support.ApiConstants.ERROR;
import static io.nats.client.support.ApiConstants.TYPE;
import static io.nats.client.support.JsonValueUtils.*;

/**
 * ApiResponse is the base class for all api responses from the server
 * @param <T> the success response class
 */
public abstract class ApiResponse<T> {

    /**
     * A constant for a response without a type
     */
    public static final String NO_TYPE = "io.nats.jetstream.api.v1.no_type";

    /**
     * a constant for a response that errors while parsing
     */
    public static final String PARSE_ERROR_TYPE = "io.nats.client.api.parse_error";

    /**
     * The json value made from creating the object from a message or that was used to directly construct the response
     */
    protected final JsonValue jv;

    private final String type;
    private Error error;

    /**
     * construct an ApiResponse from a message
     * @param msg the message
     */
    public ApiResponse(Message msg) {
        this(parseMessage(msg));
    }

    /**
     * parse the response message
     * @param msg the message
     * @return the JsonValue of the parsed JSON
     */
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

    /**
     * called when the JSON is invalid
     * @param retVal the fluent value to return
     * @return the return value
     * @param <R> the type of the return value
     */
    protected <R> R invalidJson(R retVal) {
        // only set the error if it's not already set.
        // this can easily happen when the original response is a real error
        // but the parsing continues
        if (error == null) {
            error = new Error(500, "Invalid JSON for " + getClass().getSimpleName());
        }
        return retVal;
    }

    /**
     * set an error if the value in the key is null
     * @param jv the input
     * @param key the key
     * @return the value of the key
     */
    protected String nullStringIsError(JsonValue jv, String key) {
        String s = readString(jv, key);
        return s == null ? invalidJson("") : s;
    }

    /**
     * set an error if the value in the key is null
     * @param jv the input
     * @param key the key
     * @return the value of the key
     */
    @SuppressWarnings("SameParameterValue")
    protected ZonedDateTime nullDateIsError(JsonValue jv, String key) {
        ZonedDateTime zdt = readDate(jv, key);
        return zdt == null ? invalidJson(DateTimeUtils.DEFAULT_TIME) : zdt;
    }

    /**
     * set an error if the value in the key is null
     * @param jv the input
     * @param key the key
     * @param errorValue the value in case of error
     * @return the value of the key
     */
    @SuppressWarnings("SameParameterValue")
    protected JsonValue nullValueIsError(JsonValue jv, String key, JsonValue errorValue) {
        JsonValue v = readValue(jv, key);
        return v == null ? invalidJson(errorValue) : v;
    }

    /**
     * Construct an ApiResponse from a JsonValue
     * @param jsonValue the value
     */
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

    /**
     * Construct an empty ApiResponse
     */
    public ApiResponse() {
        jv = null;
        error = null;
        type = NO_TYPE;
    }

    /**
     * Construct an ApiResponse from an error object
     * @param error the error object
     */
    public ApiResponse(Error error) {
        jv = null;
        this.error = error;
        type = NO_TYPE;
    }

    /**
     * throw an Exception if the response had an error
     * @return the ApiResponse if not an error
     * @throws JetStreamApiException if the response had an error
     */
    @SuppressWarnings("unchecked")
    public T throwOnHasError() throws JetStreamApiException {
        if (error != null) {
            throw new JetStreamApiException(error);
        }
        return (T)this;
    }

    /**
     * Get the JsonValue used to make this object
     * @return the value
     */
    @Nullable
    public JsonValue getJv() {
        return jv;
    }

    /**
     * Does the response have an error
     * @return true if the response has an error
     */
    public boolean hasError() {
        return error != null;
    }

    /**
     * The type of the response object
     * @return the type
     */
    @Nullable
    public String getType() {
        return type;
    }

    /**
     * The request error code from the server
     * @return the code
     */
    public int getErrorCode() {
        return error == null ? Error.NOT_SET : error.getCode();
    }

    /**
     * The api error code from the server
     * @return the code
     */
    public int getApiErrorCode() {
        return error == null ? Error.NOT_SET : error.getApiErrorCode();
    }

    /**
     * Get the error description
     * @return the description if the response is an error
     */
    @Nullable
    public String getDescription() {
        return error == null ? null : error.getDescription();
    }

    /**
     * Get the error object string
     * @return the error object string if the response is an error
     */
    @Nullable
    public String getError() {
        return error == null ? null : error.toString();
    }

    /**
     * Get the error object
     * @return the error object if the response is an error
     */
    @Nullable
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
