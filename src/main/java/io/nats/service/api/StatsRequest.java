// Copyright 2022 The NATS Authors
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

package io.nats.service.api;

import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonUtils;

import static io.nats.client.support.ApiConstants.INTERNAL;
import static io.nats.client.support.ApiConstants.INTERNAL_RE;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class StatsRequest implements JsonSerializable {
    public static final byte[] INTERNAL_STATS_REQUEST_BYTES = new StatsRequest(true).serialize();

    private final boolean internal;

    public StatsRequest(boolean internal) {
        this.internal = internal;
    }

    public StatsRequest(byte[] data) {
        if (data == null) {
            internal = false;
        }
        else {
            internal = JsonUtils.readBoolean(new String(data), INTERNAL_RE);
        }
    }

    public StatsRequest(String json) {
        internal = JsonUtils.readBoolean(json, INTERNAL_RE);
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addFldWhenTrue(sb, INTERNAL, internal);
        return endJson(sb).toString();
    }

    public boolean isInternal() {
        return internal;
    }

    @Override
    public String toString() {
        return toJson();
    }
}
