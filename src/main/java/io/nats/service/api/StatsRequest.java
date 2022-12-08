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

import static io.nats.client.support.JsonUtils.*;

/**
 * TBD
 */
public class StatsRequest implements JsonSerializable {
    private final boolean internal;

    public StatsRequest(boolean internal) {
        this.internal = internal;
    }

    public StatsRequest(byte[] data) {
        if (data == null) {
            internal = false;
        }
        else {
            internal = JsonUtils.readBoolean(new String(data), boolean_pattern("internal"));
        }
    }

    public StatsRequest(String json) {
        internal = JsonUtils.readBoolean(json, boolean_pattern("internal"));
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addFldWhenTrue(sb, "internal", internal);
        return endJson(sb).toString();
    }

    public boolean isInternal() {
        return internal;
    }
}
