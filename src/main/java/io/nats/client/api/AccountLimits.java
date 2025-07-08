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

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonValueUtils.readBoolean;
import static io.nats.client.support.JsonValueUtils.readLong;

/**
 * Represents the JetStream Account Limits
 */
public class AccountLimits {

    private final long maxMemory;
    private final long maxStorage;
    private final long maxStreams;     // should be an int
    private final long maxConsumers;   // should be an int
    private final long maxAckPending;  // should be an int
    private final long memoryMaxStreamBytes;
    private final long storageMaxStreamBytes;
    private final boolean maxBytesRequired;

    AccountLimits(JsonValue vAccountLimits) {
        this.maxMemory = readLong(vAccountLimits, MAX_MEMORY, 0);
        this.maxStorage = readLong(vAccountLimits, MAX_STORAGE, 0);
        this.maxStreams = readLong(vAccountLimits, MAX_STREAMS, 0);
        this.maxConsumers = readLong(vAccountLimits, MAX_CONSUMERS, 0);
        this.maxAckPending = readLong(vAccountLimits, MAX_ACK_PENDING, 0);
        this.memoryMaxStreamBytes = readLong(vAccountLimits, MEMORY_MAX_STREAM_BYTES, 0);
        this.storageMaxStreamBytes = readLong(vAccountLimits, STORAGE_MAX_STREAM_BYTES, 0);
        this.maxBytesRequired = readBoolean(vAccountLimits, MAX_BYTES_REQUIRED);
    }

    /**
     * The maximum amount of Memory storage Stream Messages may consume.
     * @return bytes
     */
    public long getMaxMemory() {
        return maxMemory;
    }

    /**
     * The maximum amount of File storage Stream Messages may consume.
     * @return bytes
     */
    public long getMaxStorage() {
        return maxStorage;
    }

    /**
     * The maximum number of Streams an account can create.
     * @return stream maximum count
     */
    public long getMaxStreams() {
        return maxStreams;
    }

    /**
     * The maximum number of Consumers an account can create.
     * @return consumer maximum count
     */
    public long getMaxConsumers() {
        return maxConsumers;
    }

    /**
     * The maximum number of outstanding ACKs any consumer may configure.
     * @return the configuration count
     */
    public long getMaxAckPending() {
        return maxAckPending;
    }

    /**
     * The maximum size any single memory stream may be.
     * @return bytes
     */
    public long getMemoryMaxStreamBytes() {
        return memoryMaxStreamBytes;
    }

    /**
     * The maximum size any single storage based stream may be.
     * @return bytes
     */
    public long getStorageMaxStreamBytes() {
        return storageMaxStreamBytes;
    }

    /**
     * Indicates if streams created in this account requires the max_bytes property set.
     * @return the flag
     */
    public boolean isMaxBytesRequired() {
        return maxBytesRequired;
    }
}
