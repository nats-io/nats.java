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
import io.nats.client.support.Status;

import static io.nats.client.support.ApiConstants.*;

/**
 * Error returned from an api request.
 */
public class Error {

    public static final int NOT_SET = -1;

    private final String json;
    private final int code;
    private final int apiErrorCode;
    private final String desc;

    static Error optionalInstance(String json) {
        String errorJson = JsonUtils.getJsonObject(ERROR, json, null);
        return errorJson == null ? null : new Error(errorJson);
    }

    Error(String json) {
        this.json = json;
        code = JsonUtils.readInt(json, CODE_RE, NOT_SET);
        apiErrorCode = JsonUtils.readInt(json, ERR_CODE_RE, NOT_SET);
        desc = JsonUtils.readString(json, DESCRIPTION_RE, "Unknown JetStream Error");
    }

    Error(int code, int apiErrorCode, String desc) {
        this.json = null;
        this.code = code;
        this.apiErrorCode = apiErrorCode;
        this.desc = desc;
    }

    public int getCode() {
        return code;
    }

    public int getApiErrorCode() {
        return apiErrorCode;
    }

    public String getDescription() {
        return desc;
    }

    @Override
    public String toString() {
        if (apiErrorCode == NOT_SET) {
            if (code == NOT_SET) {
                return desc;
            }
            return desc + " (" + code + ")";
        }

        if (code == NOT_SET) {
            return desc;
        }

        return desc + " [" + apiErrorCode + "]";
    }

    public static Error convert(Status status) {
        switch (status.getCode()) {
            case 404:
                return JSNoMessageFoundErr;
            case 408:
                return JSBadRequestErr;
        }
        return new Error(status.getCode(), NOT_SET, status.getMessage());
    }

    public static final Error JSBadRequestErr = new Error(400, 10003, "bad request");
    public static final Error JSNoMessageFoundErr = new Error(404, 10037, "no message found");
}
