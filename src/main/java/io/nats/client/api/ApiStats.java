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

import io.nats.client.support.JsonValue;
import io.nats.client.support.JsonValueUtils;

import static io.nats.client.support.ApiConstants.*;

/**
 * Represents the JetStream Account Api Stats
 */
public class ApiStats {

    private final int level;
    private final long total;
    private final long errors;
    private final long inFlight;

    ApiStats(JsonValue vApiStats) {
        this.level = JsonValueUtils.readInteger(vApiStats, LEVEL, 0);
        this.total = JsonValueUtils.readLong(vApiStats, TOTAL, 0);
        this.errors = JsonValueUtils.readLong(vApiStats, ERRORS, 0);
        this.inFlight = JsonValueUtils.readLong(vApiStats, INFLIGHT, 0);
    }

    public int getLevel() {
        return level;
    }

    /**
     * Total number of API requests received for this account
     * @return the total requests
     */
    public long getTotalApiRequests() {
        return total;
    }

    /**
     * API requests that resulted in an error response
     * @return the error count
     */
    public long getErrorCount() {
        return errors;
    }

    public long getInFlight() {
        return inFlight;
    }

    /**
     * @deprecated Deprecated, replaced with getTotalApiRequests
     * Total number of API requests received for this account
     * @return the total requests
     */
    @Deprecated
    public int getTotal() {
        return (int)total;
    }

    /**
     * @deprecated Deprecated, replaced with getErrorErroredRequests
     * API requests that resulted in an error response
     * @return the error count
     */
    @Deprecated
    public int getErrors() {
        return (int)errors;
    }
}
