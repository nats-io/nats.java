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

import static io.nats.client.support.ApiConstants.ERRORS_RE;
import static io.nats.client.support.ApiConstants.TOTAL_RE;

/**
 * Represents the JetStream Account Api Stats
 */
public class ApiStats {

    private final long total;
    private final long errors;

    ApiStats(String json) {
        this.total = JsonUtils.readLong(json, TOTAL_RE, 0);
        this.errors = JsonUtils.readLong(json, ERRORS_RE, 0);
    }

    public long getTotal() {
        return total;
    }

    public long getErrors() {
        return errors;
    }

    @Override
    public String toString() {
        return "ApiStats{" +
            "total=" + total +
            ", errors=" + errors +
            '}';
    }
}
