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

import io.nats.client.JetStreamApiException;
import io.nats.client.Message;
import io.nats.client.api.ApiResponse;
import io.nats.client.support.JsonUtils;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static io.nats.client.support.ApiConstants.*;

class ListRequestEngine extends ApiResponse<ListRequestEngine> {

    private static final String OFFSET_JSON_START = "{\"offset\":";

    protected int total = Integer.MAX_VALUE; // so always has the first "at least one more"
    protected int limit = 0;
    protected int lastOffset = 0;

    ListRequestEngine() {
        super();
    }

    ListRequestEngine(Message msg) throws JetStreamApiException {
        super(msg);
        if (hasError()) {
            throw new JetStreamApiException(this);
        }
        total = JsonUtils.readInt(json, TOTAL_RE, Integer.MAX_VALUE);
        limit = JsonUtils.readInt(json, LIMIT_RE, 0);
        lastOffset = JsonUtils.readInt(json, OFFSET_RE, 0);
    }

    boolean hasMore() {
        return total > (lastOffset + limit);
    }

    byte[] internalNextJson() {
        return hasMore() ? noFilterJson() : null;
    }

    byte[] noFilterJson() {
        return (OFFSET_JSON_START + (lastOffset + limit) + "}").getBytes(StandardCharsets.US_ASCII);
    }

    byte[] internalNextJson(String fieldName, String filter) {
        if (hasMore()) {
            if (filter == null) {
                return noFilterJson();
            }
            return (OFFSET_JSON_START + (lastOffset + limit)
                    + ",\"" + fieldName + "\":\"" + filter + "\"}").getBytes(StandardCharsets.US_ASCII);
        }
        return null;
    }

    List<String> getObjectList(String objectName) {
        return JsonUtils.getObjectList(objectName, json);
    }

    List<String> getStringList(String objectName) {
        return JsonUtils.getStringList(objectName, json);
    }
}
