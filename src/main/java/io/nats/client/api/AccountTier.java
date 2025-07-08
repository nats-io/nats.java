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
import org.jetbrains.annotations.NotNull;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonValueUtils.readInteger;
import static io.nats.client.support.JsonValueUtils.readObject;

/**
 * Represents the JetStream Account Tier
 */
public class AccountTier {

    private final long memory;
    private final long storage;
    private final long reservedMemory;
    private final long reservedStorage;
    private final int streams;
    private final int consumers;
    private final AccountLimits limits;

    AccountTier(JsonValue vAccountTier) {
        memory = readInteger(vAccountTier, MEMORY, 0);
        storage = readInteger(vAccountTier, STORAGE, 0);
        reservedMemory = readInteger(vAccountTier, RESERVED_MEMORY, 0);
        reservedStorage = readInteger(vAccountTier, RESERVED_STORAGE, 0);
        streams = readInteger(vAccountTier, STREAMS, 0);
        consumers = readInteger(vAccountTier, CONSUMERS, 0);
        limits = new AccountLimits(readObject(vAccountTier, LIMITS));
    }

    /**
     * Memory Storage being used for Stream Message storage in this tier.
     * @return the memory storage in bytes
     */
    public long getMemoryBytes() {
        return memory;
    }

    /**
     * File Storage being used for Stream Message storage in this tier.
     * @return the storage in bytes
     */
    public long getStorageBytes() {
        return storage;
    }

    /**
     * Bytes that is reserved for memory usage by this account on the server
     * @return the memory usage in bytes
     */
    public long getReservedMemory() {
        return (int)reservedMemory;
    }

    /**
     * Bytes that is reserved for disk usage by this account on the server
     * @return the disk usage in bytes
     */
    public long getReservedStorage() {
        return reservedStorage;
    }

    /**
     * Bytes that is reserved for memory usage by this account on the server
     * @return the memory usage in bytes
     */
    public long getReservedMemoryBytes() {
        return reservedMemory;
    }

    /**
     * Bytes that is reserved for disk usage by this account on the server
     * @return the disk usage in bytes
     */
    public long getReservedStorageBytes() {
        return reservedStorage;
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
    @NotNull
    public AccountLimits getLimits() {
        return limits;
    }

    /**
     * @deprecated use getMemoryBytes instead
     * Memory Storage being used for Stream Message storage in this tier.
     * @return the memory storage in bytes
     */
    @Deprecated
    public int getMemory() {
        return (int)memory;
    }

    /**
     * @deprecated use getStorageBytes instead
     * File Storage being used for Stream Message storage in this tier.
     * @return the storage in bytes
     */
    @Deprecated
    public int getStorage() {
        return (int)storage;
    }
}
