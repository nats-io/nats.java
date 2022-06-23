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

import static io.nats.client.support.ApiConstants.*;

/**
 * Represents the JetStream Account Tier
 */
public class AccountTier {

    private final int memory;
    private final int storage;
    private final int streams;
    private final int consumers;
    private final AccountLimits limits;

    AccountTier(String json) {
        memory = JsonUtils.readInt(json, MEMORY_RE, 0);
        storage = JsonUtils.readInt(json, STORAGE_RE, 0);
        streams = JsonUtils.readInt(json, STREAMS_RE, 0);
        consumers = JsonUtils.readInt(json, CONSUMERS_RE, 0);
        limits = new AccountLimits(JsonUtils.getJsonObject(LIMITS, json));
    }

    /**
     * Memory Storage being used for Stream Message storage in this tier.
     * @return the storage in bytes
     */
    public int getMemory() {
        return memory;
    }

    /**
     * File Storage being used for Stream Message storage in this tier.
     * @return the storage in bytes
     */
    public int getStorage() {
        return storage;
    }

    /**
     * Number of active streams in this tier.
     * @return the number of streams
     */
    public int getStreams() {
        return streams;
    }

    /**
     * Number of active consumers in this tier.
     * @return the number of consumers
     */
    public int getConsumers() {
        return consumers;
    }

    /**
     * The limits of this tier.
     * @return the limits object
     */
    public AccountLimits getLimits() {
        return limits;
    }
}
