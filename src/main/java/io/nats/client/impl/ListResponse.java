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

public class ListResponse {
    private static final Pattern TOTAL_RE = JsonUtils.buildPattern("total", JsonUtils.FieldType.jsonNumber);
    private static final Pattern OFFSET_RE = JsonUtils.buildPattern("offset", JsonUtils.FieldType.jsonNumber);
    private static final Pattern LIMIT_RE = JsonUtils.buildPattern("limit", JsonUtils.FieldType.jsonNumber);

    private static final String OFFSET_JSON_START = "{\"offset\":";

    protected int total = Integer.MAX_VALUE; // so always has the first "at least one more"
    protected int limit = 0;
    protected int lastOffset = 0;

    public void update(String json) {
        Matcher m = TOTAL_RE.matcher(json);
        if (m.find()) {
            this.total = Integer.parseInt(m.group(1));
        }

        m = OFFSET_RE.matcher(json);
        if (m.find()) {
            this.lastOffset = Integer.parseInt(m.group(1));
        }

        m = LIMIT_RE.matcher(json);
        if (m.find()) {
            this.limit = Integer.parseInt(m.group(1));
        }
    }

    public boolean hasMore() {
        return total > (lastOffset + limit);
    }

    String internalNextJson() {
        return hasMore() ? noFilterJson() : null;
    }

    private String noFilterJson() {
        return OFFSET_JSON_START + (lastOffset + limit) + "}";
    }

    String internalNextJson(String fieldName, String filter) {
        if (hasMore()) {
            if (filter == null) {
                return noFilterJson();
            }
            return OFFSET_JSON_START + (lastOffset + limit)
                    + ",\"" + fieldName + "\":\"" + filter + "\"}";
        }
        return null;
    }
}
