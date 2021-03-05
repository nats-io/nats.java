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

import java.nio.charset.StandardCharsets;

import static io.nats.client.support.ApiConstants.*;

public class JetStreamApiResponse {

    public static final int NOT_SET = -1;
    public static final String NO_TYPE = "io.nats.jetstream.api.v1.no_type";

    private final String response;
    private final boolean hasError;

    private String type;
    private Integer errorCode;
    private String errorDesc;

    public JetStreamApiResponse(Message msg) {
        this(msg.getData());
    }

    public JetStreamApiResponse(byte[] rawResponse) {
        response = new String(rawResponse, StandardCharsets.UTF_8);
        hasError = response.contains("\"error\"");
    }

    public boolean hasError() {
        return hasError;
    }

    public String getType() {
        if (type == null) {
            type = JsonUtils.readString(response, TYPE_RE, NO_TYPE);
        }
        return type;
    }

    public long getErrorCode() {
        if (errorCode == null) {
            errorCode = JsonUtils.readInt(response, CODE_RE, NOT_SET);
        }
        return errorCode;
    }

    public String getDescription() {
        if (errorDesc == null) {
            errorDesc = JsonUtils.readString(response, DESCRIPTION_RE, NatsConstants.EMPTY);
        }
        return errorDesc.length() == 0 ? null : errorDesc;
    }

    public String getError() {
        if (hasError()) {
            if (getDescription() == null) {
                return getErrorCode() == NOT_SET
                        ? "Unknown JetStream Error: " + response
                        : "Unknown JetStream Error (" + errorCode + ")";
            }

            return errorDesc + " (" + getErrorCode() + ")";
        }
        return null;
    }

    public String getResponse() {
        return response;
    }
}
