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
import io.nats.client.support.JsonUtils;

import java.nio.charset.StandardCharsets;

import static io.nats.client.api.Error.NOT_SET;
import static io.nats.client.support.ApiConstants.TYPE_RE;

public abstract class ApiResponse<T> {

    public static final String NO_TYPE = "io.nats.jetstream.api.v1.no_type";

    protected final String json;

    private String type;
    private Error error;

    public ApiResponse(Message msg) {
        this(new String(msg.getData(), StandardCharsets.UTF_8));
    }

    public ApiResponse(String json) {
        this.json = json;
        error = Error.optionalInstance(json);
    }

    public ApiResponse() {
        json = null;
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
        if (type == null) {
            type = JsonUtils.readString(json, TYPE_RE, NO_TYPE);
        }
        return type;
    }

    public long getErrorCode() {
        return error == null ? NOT_SET : error.getCode();
    }

    public String getDescription() {
        return error == null ? null : error.getDescription();
    }

    public String getError() {
        if (hasError()) {
            return error.toString();
        }
        return null;
    }
}
