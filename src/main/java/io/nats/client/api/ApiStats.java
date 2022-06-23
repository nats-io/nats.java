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

    private final int total;
    private final int errors;

    ApiStats(String json) {
        this.total = JsonUtils.readInt(json, TOTAL_RE, 0);
        this.errors = JsonUtils.readInt(json, ERRORS_RE, 0);
    }

    /**
     * Total number of API requests received for this account
     * @return the total requests
     */
    public int getTotal() {
        return total;
    }

    /**
     * API requests that resulted in an error response
     * @return the error count
     */
    public int getErrors() {
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
