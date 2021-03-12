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

package io.nats.client.impl;

import io.nats.client.Message;
import io.nats.client.support.NatsConstants;

import static io.nats.client.support.ApiConstants.*;

public abstract class JetStreamApiResponse<T> {

    public static final int NOT_SET = -1;
    public static final String NO_TYPE = "io.nats.jetstream.api.v1.no_type";

    protected final String json;

    private final boolean hasError;
    private String type;
    private Integer errorCode;
    private String errorDesc;

    public JetStreamApiResponse(Message msg) {
        this(JsonUtils.decode(msg.getData()));
    }

    JetStreamApiResponse(String json) {
        this.json = json;
        hasError = json.contains("\"error\"");
    }

    JetStreamApiResponse() {
        json = null;
        hasError = false;
    }

    @SuppressWarnings("unchecked")
    public T throwOnHasError() throws JetStreamApiException {
        if (hasError()) {
            throw new JetStreamApiException(this);
        }
        return (T)this;
    }

    public boolean hasError() {
        return hasError;
    }

    public String getType() {
        if (type == null) {
            type = JsonUtils.readString(json, TYPE_RE, NO_TYPE);
        }
        return type;
    }

    public long getErrorCode() {
        if (errorCode == null) {
            errorCode = JsonUtils.readInt(json, CODE_RE, NOT_SET);
        }
        return errorCode;
    }

    public String getDescription() {
        if (errorDesc == null) {
            errorDesc = JsonUtils.readString(json, DESCRIPTION_RE, NatsConstants.EMPTY);
        }
        return errorDesc.length() == 0 ? null : errorDesc;
    }

    public String getError() {
        if (hasError()) {
            if (getDescription() == null) {
                return getErrorCode() == NOT_SET
                        ? "Unknown JetStream Error: " + json
                        : "Unknown JetStream Error (" + errorCode + ")";
            }

            return errorDesc + " (" + getErrorCode() + ")";
        }
        return null;
    }
}
