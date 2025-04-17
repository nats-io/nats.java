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
import io.nats.client.support.JsonValue;

import java.util.HashMap;
import java.util.Map;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonValueUtils.readObject;
import static io.nats.client.support.JsonValueUtils.readString;

/**
 * The JetStream Account Statistics
 */
public class AccountStatistics
        extends ApiResponse<AccountStatistics> {

    private final AccountTier rollupTier;
    private final String domain;
    private final ApiStats api;
    private final Map<String, AccountTier> tiers;

    public AccountStatistics(Message msg) {
        super(msg);
        rollupTier = new AccountTier(jv);
        domain = readString(jv, DOMAIN);
        api = new ApiStats(readObject(jv, API));
        JsonValue vTiers = readObject(jv, TIERS);
        tiers = new HashMap<>();
        if (vTiers.map != null) {
            for (String key : vTiers.map.keySet()) {
                tiers.put(key, new AccountTier(vTiers.map.get(key)));
            }
        }
    }

    /**
     * Gets the amount of memory storage used by the JetStream deployment.
     * If the account has tiers, this will represent a rollup.
     * @return bytes
     */
    public long getMemory() {
        return rollupTier.getMemoryBytes();
    }

    /**
     * Gets the amount of file storage used by  the JetStream deployment.
     * If the account has tiers, this will represent a rollup.
     * @return bytes
     */
    public long getStorage() {
        return rollupTier.getStorageBytes();
    }

    /**
     * Bytes that is reserved for memory usage by this account on the server
     * @return the memory usage in bytes
     */
    public long getReservedMemory() {
        return rollupTier.getReservedMemoryBytes();
    }

    /**
     * Bytes that is reserved for disk usage by this account on the server
     * @return the disk usage in bytes
     */
    public long getReservedStorage() {
        return rollupTier.getReservedStorageBytes();
    }

    /**
     * Gets the number of streams used by the JetStream deployment.
     * If the account has tiers, this will represent a rollup.
     * @return stream maximum count
     */
    public long getStreams() {
        return rollupTier.getStreams();
    }

    /**
     * Gets the number of consumers used by the JetStream deployment.
     * If the account has tiers, this will represent a rollup.
     * @return consumer maximum count
     */
    public long getConsumers() {
        return rollupTier.getConsumers();
    }

    /**
     * Gets the Account Limits object. If the account has tiers,
     * the object will be present but all values will be zero.
     * See the Account Limits for the specific tier.
     * @return the AccountLimits object
     */
    public AccountLimits getLimits() {
        return rollupTier.getLimits();
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
}
