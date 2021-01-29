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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.nats.client.impl.JsonUtils.buildPattern;

public class JetStreamApiResponse {

    private static final int NOT_ERROR = -1;

    private int code = NOT_ERROR;
    private String desc;
    private String type;
    private String response;

    private static final Pattern typeRE = buildPattern("type", FieldType.jsonString);
    private static final Pattern codeRE = buildPattern("code", FieldType.jsonNumber);
    private static final Pattern descRE = buildPattern("description", FieldType.jsonString);

    public JetStreamApiResponse(Message msg) {
        this(msg.getData());
    }

    public JetStreamApiResponse(byte[] rawResponse) {

        response = new String(rawResponse);

        Matcher m = typeRE.matcher(response);
        if (m.find()) {
            this.type = m.group(1);
        }

        String error = JsonUtils.getJSONObject("error", response);
        m = codeRE.matcher(error);
        if (m.find()) {
            this.code = Integer.parseInt(m.group(1));
        }

        m = descRE.matcher(error);
        if (m.find()) {
            this.desc = m.group(1);
        }
    }

    public String getType() {
        return type;
    }

    public long getCode() {
        return code;
    }

    public boolean hasError() {
        return code != NOT_ERROR;
    }

    public String getError() {
        if (code == NOT_ERROR) {
            return null;
        }
        if (desc == null) {
            return code == 0
                    ? "Unknown Jetstream Error: " + response
                    : "Unknown Jetstream Error (" + code + ")";
        }
        return desc + " (" + code + ")";
    }

    public String getDescription() {
        return desc;
    }

    public String getResponse() {
        return response;
    }

    static boolean isError(String s) {
        return s.contains("code");
    }
}
