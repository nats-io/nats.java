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
import io.nats.client.impl.JsonUtils.FieldType;

import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.nats.client.impl.JsonUtils.buildPattern;

public class JetStreamApiResponse {

    public static final int NOT_SET = -1;
    public static final String NOT_TYPED = "io.nats.jetstream.api.v1.no_type";

    private final String response;
    private final boolean hasError;

    private String errorJson;

    private String type;
    private Integer errorCode;
    private String errorDesc;

    private static final Pattern typeRE = buildPattern("type", FieldType.jsonString);
    private static final Pattern codeRE = buildPattern("code", FieldType.jsonNumber);
    private static final Pattern descRE = buildPattern("description", FieldType.jsonString);

    public JetStreamApiResponse(Message msg) {
        this(msg.getData());
    }

    public JetStreamApiResponse(byte[] rawResponse) {
        response = new String(rawResponse, StandardCharsets.UTF_8);
        hasError = response.contains("\"error\"");
    }

    protected String ensureErrorJson() {
        if (errorJson == null) {
            errorJson = JsonUtils.getJSONObject("error", response);
        }
        return errorJson;
    }

    public boolean hasError() {
        return hasError;
    }

    public String getType() {
        if (type == null) {
            Matcher m = typeRE.matcher(response);
            if (m.find()) {
                type = m.group(1);
            }
            else {
                type = NOT_TYPED;
            }
        }
        return type;
    }

    public long getErrorCode() {
        if (errorCode == null) {
            Matcher m = codeRE.matcher(ensureErrorJson());
            if (m.find()) {
                errorCode = Integer.parseInt(m.group(1));
            }
            else {
                errorCode = NOT_SET;
            }
        }
        return errorCode;
    }

    public String getDescription() {
        if (errorDesc == null) {
            Matcher m = descRE.matcher(ensureErrorJson());
            if (m.find()) {
                this.errorDesc = m.group(1);
            }
            else {
                errorDesc = "";
            }
        }
        return errorDesc.length() == 0 ? null : errorDesc;
    }

    public String getError() {
        if (hasError()) {
            if (getDescription() == null) {
                return getErrorCode() == NOT_SET
                        ? "Unknown Jetstream Error: " + response
                        : "Unknown Jetstream Error (" + errorCode + ")";
            }

            return errorDesc + " (" + getErrorCode() + ")";
        }
        return null;
    }

    public String getResponse() {
        return response;
    }
}
