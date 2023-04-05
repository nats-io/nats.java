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

import io.nats.client.support.Validator;

/**
 * TODO
 */
public class ConsumeOptions {
    public static final int DEFAULT_BATCH_SIZE = 100;
    public static final int DEFAULT_THRESHOLD_PERCENT = 25;
    public static final long DEFAULT_EXPIRES_IN_MS = 30000;
    public static final long DEFAULT_IDLE_HEARTBEAT_MS = 15000;

    public static final ConsumeOptions DEFAULT_OPTIONS = builder().build();

    private final int batchSize;
    private final int maxBytes;
    private final long expiresIn;
    private final long idleHeartbeat;
    private final int thresholdMessages;
    private final int thresholdBytes;

    // todo minimums and validation
    private ConsumeOptions(Builder b) {
        this.batchSize = b.batchSize;
        this.maxBytes = b.maxBytes;
        this.expiresIn = b.expiresIn;
        this.idleHeartbeat = b.idleHeartbeat;

        thresholdMessages = Math.max(1, batchSize * b.thresholdPct / 100);
        thresholdBytes = maxBytes > 0 ? Math.max(1, maxBytes * b.thresholdPct / 100) : Integer.MAX_VALUE;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getMaxBytes() {
        return maxBytes;
    }

    public long getExpiresInMillis() {
        return expiresIn;
    }

    public long getIdleHeartbeatMillis() {
        return idleHeartbeat;
    }

    public int getThresholdMessages() {
        return thresholdMessages;
    }

    public int getThresholdBytes() {
        return thresholdBytes;
    }

    /**
     * Creates a builder for the consume options
     * @return a ConsumeOptions builder
     */
    public static Builder builder() {
        return new Builder();
    }

    private static ConsumeOptions predefined(int batchSize) {
        return new Builder().batchSize(batchSize).build();
    }

    public static class Builder {
        private int batchSize = DEFAULT_BATCH_SIZE;
        private int maxBytes = -1;
        private int thresholdPct = DEFAULT_THRESHOLD_PERCENT;
        private long expiresIn = DEFAULT_EXPIRES_IN_MS;
        private long idleHeartbeat = DEFAULT_IDLE_HEARTBEAT_MS;
        private boolean ordered;

        /**
         * Set the maximum number of messages to consume for Fetch
         * or the batch size for each pull during Consume.
         * @param batchSize the size of the batch. Must be greater than 0
         * @return the builder
         */
        public Builder batchSize(int batchSize) {
            this.batchSize = Validator.validateGtZero(batchSize, "Batch Size");
            return this;
        }

        /**
         * The maximum bytes to consume for Fetch or the maximum bytes for each pull during Consume.
         * When set (a value greater than zero,) it is used in conjunction with batch size, meaning
         * whichever limit is reached first is respected.
         * @param maxBytes the maximum bytes
         * @return the builder
         */
        public Builder maxBytes(int maxBytes) {
            this.maxBytes = (int)Validator.validateGtEqZero(maxBytes, "Max Bytes");
            return this;
        }

        /**
         * In Fetch, sets the maximum amount of time to wait to reach the batch size or max byte.
         * In Consume, sets the maximum amount of time for an individual pull to be open
         * before issuing a replacement pull.
         * @param expiresInMillis the millis
         * @return the builder
         */
        public Builder expiresIn(long expiresInMillis) {
            this.expiresIn = expiresInMillis;
            return idleHeartbeat(expiresInMillis / 2);
        }

        /**
         * Set the idle heartbeat time in millis
         * @param idleHeartbeatMillis the millis
         * @return the builder
         */
        public Builder idleHeartbeat(long idleHeartbeatMillis) {
            this.idleHeartbeat = idleHeartbeatMillis;
            return this;
        }

        /**
         * Set the threshold percent of max bytes (if max bytes is specified) or messages
         * that will trigger issuing pull requests to keep messages flowing.
         * Only applies to endless consumes
         * For instance if the batch size is 100 and the re-pull percent is 25,
         * the first pull will be for 100, and then when 25 messages have been received
         * another 75 will be requested, keeping the number of messages in transit always at 100.
         * @param thresholdPct the percent from 1 to 100 inclusive.
         * @return the builder
         */
        public Builder thresholdPercent(int thresholdPct) {
            if (thresholdPct < 1 || thresholdPct > 100) {
                throw new IllegalArgumentException("Threshold percent must be between 1 and 100 inclusive.");
            }
            this.thresholdPct = thresholdPct;
            return this;
        }

        /**
         * Build the ConsumeOptions.
         * @return the built ConsumeOptions
         */
        public ConsumeOptions build() {
            return new ConsumeOptions(this);
        }
    }
}
