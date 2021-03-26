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

import io.nats.client.support.JsonUtils;

import static io.nats.client.support.ApiConstants.*;

/**
 * Error returned from an api request.
 */
public class Error {

    public static final int NOT_SET = -1;

    private final String json;
    private final Integer code;
    private final String desc;

    static Error optionalInstance(String json) {
        String errorJson = JsonUtils.getJsonObject(ERROR, json, null);
        return errorJson == null ? null : new Error(errorJson);
    }

    Error(String json) {
        this.json = json;
        code = JsonUtils.readInt(json, CODE_RE, NOT_SET);
        desc = JsonUtils.readString(json, DESCRIPTION_RE, null);
    }

    public long getCode() {
        return code;
    }

    public String getDescription() {
        return desc;
    }

    @Override
    public String toString() {
        if (desc == null) {
            return code == NOT_SET
                    ? "Unknown JetStream Error: " + json
                    : "Unknown JetStream Error (" + code + ")";
        }

        return desc+ " (" + code + ")";
    }
}
