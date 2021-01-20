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

class JetstreamAPIResponse {

    private static final int UnsetErrorCode = -1;

    private int code = UnsetErrorCode;
    private String desc = null;
    private String type = null;
    private String response = null;

    private static String typeField = "type";
    private static String codeField = "code";
    private static String descField = "description";

    private static final Pattern typeRE = JsonUtils.buildPattern(typeField, FieldType.jsonString);
    private static final Pattern codeRE = JsonUtils.buildPattern(codeField, FieldType.jsonNumber);
    private static final Pattern descRE = JsonUtils.buildPattern(descField, FieldType.jsonString);

    JetstreamAPIResponse(Message msg) {
        this(msg.getData());
    }

    JetstreamAPIResponse(byte[] rawResponse) {

        response = new String(rawResponse);

        Matcher m = typeRE.matcher(response);
        if (m.find()) {
            this.type = m.group(1);
        }

        m = codeRE.matcher(response);
        if (m.find()) {
            this.code = Integer.parseInt(m.group(1));
        }

        m = descRE.matcher(response);
        if (m.find()) {
            this.desc = m.group(1);
        }
    }

    String getType() {
        return type;
    }

    long getCode() {
        return code;
    }

    boolean hasError() {
        return code != UnsetErrorCode;
    }

    String getError() {
        if (code == UnsetErrorCode) {
            return null;
        }
        if (desc == null && code == 0) {
            return "Unknown Jetstream Error: " + getResponse();
        }
        if (desc == null && code > 0) {
            return "Unknown Jetstream error with code: " + getCode();
        }
        return desc;
    }

    String getResponse() {
        return response;
    }

    String getDescription() {
        return desc;
    }

    static boolean isError(String s) {
        return s.contains("code");
    }
}
