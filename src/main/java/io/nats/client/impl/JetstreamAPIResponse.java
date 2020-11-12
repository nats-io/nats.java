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

    private static final String grabString = "\\s*\"(.+?)\"";
    private static final String grabNumber = "\\s*(\\d+)";

    // TODO - replace with a safe (risk-wise) JSON parser
    private static final Pattern typeRE = Pattern.compile("\"" + typeField + "\":" + grabString,
            Pattern.CASE_INSENSITIVE);
    private static final Pattern codeRE = Pattern.compile("\"" + codeField + "\":" + grabNumber,
            Pattern.CASE_INSENSITIVE);
    private static final Pattern descRE = Pattern.compile("\"" + descField + "\":" + grabString,
            Pattern.CASE_INSENSITIVE);

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

    boolean hasError() {
        return code != UnsetErrorCode;
    }

    String getError() {
        if (code == UnsetErrorCode) {
            return null;
        }
        if (desc == null && code == 0) {
            return "Unknown Jetstream Error";
        }
        if (desc == null || code > 0) {
            return "Unknown Jetstream error.  Code: " + code;
        }
        return desc;
    }

    String getResponse() {
        if (code != UnsetErrorCode) {
            return null;
        }
        return response;
    }
}
