// Copyright 2023 The NATS Authors
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

package io.nats.client;

import java.time.Duration;

/**
 * TODO
 */
public class ConsumeOptions {
    public static final int DEFAULT_BATCH_SIZE = 100;
    public static final int DEFAULT_MAX_BYTES = 0;
    public static final int DEFAULT_REPULL_PERCENT = 25;
    public static final Duration DEFAULT_EXPIRES_IN = Duration.ofSeconds(30);
    public static final Duration DEFAULT_IDLE_HEARTBEAT = Duration.ofSeconds(15);

    public static final ConsumeOptions DEFAULT_OPTIONS = builder().build();
    public static final ConsumeOptions XLARGE_PAYLOAD = predefined(10);
    public static final ConsumeOptions LARGE_PAYLOAD = predefined(20);
    public static final ConsumeOptions MEDIUM_PAYLOAD = predefined(50);
    public static final ConsumeOptions SMALL_PAYLOAD = predefined(DEFAULT_BATCH_SIZE);
    public static final ConsumeOptions DEFAULT_FETCH_ALL_OPTIONS = builder().expiresIn(10000).build();

    private final int batchSize;
    private final int maxBytes;
    private final int repullAt;
    private final Duration expiresIn;
    private final Duration idleHeartbeat;

    public ConsumeOptions(Builder b) {
        this.batchSize = b.batchSize;
        this.maxBytes = b.maxBytes;
        this.expiresIn = b.expiresIn;
        this.idleHeartbeat = b.idleHeartbeat;

        if (maxBytes > DEFAULT_MAX_BYTES) {
            repullAt = maxBytes * b.repullPercent / DEFAULT_BATCH_SIZE;
        }
        else {
            repullAt = batchSize * b.repullPercent / DEFAULT_BATCH_SIZE;
        }
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getMaxBytes() {
        return maxBytes;
    }

    public int getRepullAt() {
        return repullAt;
    }

    public Duration getExpiresIn() {
        return expiresIn;
    }

    public Duration getIdleHeartbeat() {
        return idleHeartbeat;
    }

    /**
     * Creates a builder for the pull options, with batch size since it's always required
     * @return a pull options builder
     */
    public static Builder builder() {
        return new Builder();
    }

    private static ConsumeOptions predefined(int batchSize) {
        return new Builder().batchSize(batchSize).build();
    }

    public static class Builder {
        private int batchSize = DEFAULT_BATCH_SIZE;
        private int maxBytes = DEFAULT_MAX_BYTES;
        private int repullPercent = DEFAULT_REPULL_PERCENT;
        private Duration expiresIn = DEFAULT_EXPIRES_IN;
        private Duration idleHeartbeat = DEFAULT_IDLE_HEARTBEAT;

        /**
         * Set the batch size for the pull
         * @param batchSize the size of the batch. Must be greater than 0
         * @return the builder
         */
        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize < 1 ? DEFAULT_BATCH_SIZE : batchSize;
            return this;
        }

        /**
         * The maximum bytes for the pull
         * @param maxBytes the maximum bytes
         * @return the builder
         */
        public Builder maxBytes(int maxBytes) {
            this.maxBytes = maxBytes < 1 ? -1 : maxBytes;
            return this;
        }

        /**
         * Set the repull at. Applies to max bytes if max bytes is specified,
         * otherwise applies to batch
         * @return the builder
         */
        public Builder repullPercent(int repullPct) {
            this.repullPercent = repullPct < 1 ? DEFAULT_REPULL_PERCENT : Math.min(repullPct, 75);
            return this;
        }

        /**
         * Set the expires time in millis
         * @param expiresInMillis the millis
         * @return the builder
         */
        public Builder expiresIn(long expiresInMillis) {
            this.expiresIn = Duration.ofMillis(expiresInMillis);
            return this;
        }

        /**
         * Set the expires duration
         * @param expiresIn the duration
         * @return the builder
         */
        public Builder expiresIn(Duration expiresIn) {
            this.expiresIn = expiresIn;
            return this;
        }

        /**
         * Set the idle heartbeat time in millis
         * @param idleHeartbeatMillis the millis
         * @return the builder
         */
        public Builder idleHeartbeat(long idleHeartbeatMillis) {
            this.idleHeartbeat = Duration.ofMillis(idleHeartbeatMillis);
            return this;
        }

        /**
         * Set the idle heartbeat duration
         * @param idleHeartbeat the duration
         * @return the builder
         */
        public Builder idleHeartbeat(Duration idleHeartbeat) {
            this.idleHeartbeat = idleHeartbeat;
            return this;
        }

        /**
         * Build the SimpleConsumerOptions. Validates that the batch size is greater than 0
         * @return the built PullRequestOptions
         */
        public ConsumeOptions build() {
            return new ConsumeOptions(this);
        }
    }
}
