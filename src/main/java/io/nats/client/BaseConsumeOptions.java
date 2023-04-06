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
 * Base Consume Options are provided to customize the way the
 * consume and fetch operate. It is the base class for FetchConsumeOptions
 */
public class BaseConsumeOptions {
    public static final int DEFAULT_MESSAGE_COUNT = 100;
    public static final int DEFAULT_THRESHOLD_PERCENT = 25;
    public static final long DEFAULT_EXPIRES_IN_MS = 30000;
    public static final long MIN_EXPIRES_MILLS = 1000;
    public static final long MAX_EXPIRES_MILLIS = 60000;
    public static final int MAX_IDLE_HEARTBEAT_PCT = 50;

    protected final int messages;
    protected final int bytes;
    protected final long expiresIn;
    protected final long idleHeartbeat;
    protected final int thresholdMessages;
    protected final int thresholdBytes;

    @SuppressWarnings("rawtypes") // Don't need the type of the builder to get its vars
    protected BaseConsumeOptions(Builder b) {
        this.messages = b.messages == -1
            ? DEFAULT_MESSAGE_COUNT
            : Validator.validateGtZero(b.messages, "Batch Size or Max Message Count");

        this.bytes = b.bytes == -1 ? 0
            : (int)Validator.validateGtEqZero(b.bytes, "Max Bytes");

        int threshPct;
        if (b.thresholdPct == -1) {
            threshPct = DEFAULT_THRESHOLD_PERCENT;
        }
        else if (b.thresholdPct >= 1 && b.thresholdPct <= 100) {
            threshPct = b.thresholdPct;
        }
        else {
            throw new IllegalArgumentException("Threshold percent must be between 1 and 100 inclusive.");
        }
        thresholdMessages = Math.max(1, messages * threshPct / 100);
        thresholdBytes = Math.max(0, bytes * threshPct / 100);

        if (b.expiresIn == -1) {
            this.expiresIn = DEFAULT_EXPIRES_IN_MS;
        }
        else if (b.expiresIn >= MIN_EXPIRES_MILLS && b.expiresIn <= MAX_EXPIRES_MILLIS) {
            this.expiresIn = b.expiresIn;
        }
        else {
            throw new IllegalArgumentException("Expires must be between 1 and 60 seconds inclusive.");
        }

        this.idleHeartbeat = this.expiresIn * MAX_IDLE_HEARTBEAT_PCT / 100;
    }

    public long getExpires() {
        return expiresIn;
    }

    public long getIdleHeartbeat() {
        return idleHeartbeat;
    }

    public int getThresholdMessages() {
        return thresholdMessages;
    }

    public int getThresholdBytes() {
        return thresholdBytes;
    }

    protected static abstract class Builder<B, CO> {
        private int messages = -1;
        private int bytes = -1;
        private int thresholdPct = -1;
        private long expiresIn = -1;

        protected abstract B getThis();

        protected B messages(int messages) {
            this.messages = messages;
            return getThis();
        }

        protected B bytes(int bytes, int messages) {
            this.bytes = bytes;
            this.messages = messages;
            return getThis();
        }

        /**
         * In Fetch, sets the maximum amount of time to wait to reach the batch size or max byte.
         * In Consume, sets the maximum amount of time for an individual pull to be open
         * before issuing a replacement pull.
         * @param expiresInMillis the millis
         * @return the builder
         */
        public B expiresIn(long expiresInMillis) {
            this.expiresIn = expiresInMillis;
            return getThis();
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
        public B thresholdPercent(int thresholdPct) {
            this.thresholdPct = thresholdPct;
            return getThis();
        }

        /**
         * Build the ConsumeOptions.
         * @return the built ConsumeOptions
         */
        public abstract CO build();
    }
}
