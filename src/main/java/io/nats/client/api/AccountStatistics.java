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

import io.nats.client.Message;
import io.nats.client.support.JsonUtils;

import java.util.HashMap;
import java.util.Map;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.objectString;

/**
 * The JetStream Account Statistics
 */
public class AccountStatistics
        extends ApiResponse<AccountStatistics> {

    private final AccountTier rollup;
    private final String domain;
    private final ApiStats api;
    private final Map<String, AccountTier> tiers;

    public AccountStatistics(Message msg) {
        super(msg);
        rollup = new AccountTier(json);
        domain = JsonUtils.readString(json, DOMAIN_RE);
        api = new ApiStats(JsonUtils.getJsonObject(API, json));

        String tiersJson = JsonUtils.getJsonObject(TIERS, json);
        Map<String, String> jsonByKey = JsonUtils.getMapOfObjects(tiersJson);
        tiers = new HashMap<>();
        for (String key : jsonByKey.keySet()) {
            tiers.put(key, new AccountTier(jsonByKey.get(key)));
        }
    }

    /**
     * Gets the amount of memory used by the JetStream deployment.
     *
     * @return bytes
     */
    public long getMemory() {
        return rollup.getMemory();
    }

    /**
     * Gets the amount of storage used by  the JetStream deployment.
     *
     * @return bytes
     */
    public long getStorage() {
        return rollup.getStorage();
    }

    /**
     * Gets the number of streams used by the JetStream deployment.
     *
     * @return stream maximum count
     */
    public long getStreams() {
        return rollup.getStreams();
    }

    /**
     * Gets the number of consumers used by the JetStream deployment.
     *
     * @return consumer maximum count
     */
    public long getConsumers() {
        return rollup.getConsumers();
    }

    /**
     * Gets the Account Limits object
     * @return the AccountLimits object
     */
    public AccountLimits getLimits() {
        return rollup.getLimits();
    }

    /**
     * Gets the account domain
     * @return the domain
     */
    public String getDomain() {
        return domain;
    }

    /**
     * Gets the account api stats
     * @return the ApiStats object
     */
    public ApiStats getApi() {
        return api;
    }

    /**
     * Gets the map of the Account Tiers by tier name
     * @return the map
     */
    public Map<String, AccountTier> getTiers() {
        return tiers;
    }

    @Override
    public String toString() {
        return "AccountStatsImpl{" +
            "memory=" + rollup.getMemory() +
            ", storage=" + rollup.getStorage() +
            ", streams=" + rollup.getStreams() +
            ", consumers=" + rollup.getConsumers() +
            ", " + objectString("limits", rollup.getLimits()) +
            ", domain=" + domain +
            ", " + objectString("api", api) +
            ", tiers=" + tiers.keySet() +
            '}';
    }
}
