// Copyright 2024 The NATS Authors
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

package io.nats.client.utility;

/**
 * A class to config how retries are executed.
 */
public class RetryConfig {

    public static final int DEFAULT_ATTEMPTS = 2;
    public static final long[] DEFAULT_BACKOFF_POLICY = new long[]{250, 250, 500, 500, 3000, 5000};

    // DEV NOTE. DEFAULT_CONFIG MUST BE DECLARED AFTER CONSTANTS BECAUSE
    // JAVA INITIALIZES STATICS IN THE ORDER DEFINED IN CODE
    /**
     * The default retry configuration.
     */
    public static final RetryConfig DEFAULT_CONFIG = RetryConfig.builder().build();

    private final long[] backoffPolicy;
    private final int attempts;
    private final long deadline;

    private RetryConfig(Builder b) {
        this.backoffPolicy = b.backoffPolicy;
        this.attempts = b.attempts;
        this.deadline = b.deadline;
    }

    /**
     * The configured backoff policy
     * @return the policy
     */
    public long[] getBackoffPolicy() {
        return backoffPolicy;
    }

    /**
     * The configured number of attempts
     * @return the number of attempts
     */
    public int getAttempts() {
        return attempts;
    }

    /**
     * The configured deadline
     * @return the deadline in milliseconds
     */
    public long getDeadline() {
        return deadline;
    }

    /**
     * Creates a builder for the config.
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * The builder class for the RetryConfig
     */
    public static class Builder {
        private long[] backoffPolicy = DEFAULT_BACKOFF_POLICY;
        private int attempts = DEFAULT_ATTEMPTS;
        private long deadline = Long.MAX_VALUE;

        /**
         * Set the backoff policy
         * @param backoffPolicy the policy array
         * @return the builder
         */
        public Builder backoffPolicy(long[] backoffPolicy) {
            this.backoffPolicy = backoffPolicy;
            return this;
        }

        /**
         * Set the number of times to retry
         * @param attempts the number of retry attempts
         * @return the builder
         */
        public Builder attempts(int attempts) {
            this.attempts = attempts < 1 ? DEFAULT_ATTEMPTS : attempts;
            return this;
        }

        /**
         * Set the deadline. The retry will be given at max this much time to execute
         * if expires before the number of retries.
         * @param retryDeadlineMillis the deadline time in millis
         * @return the builder
         */
        public Builder deadline(long retryDeadlineMillis) {
            this.deadline = retryDeadlineMillis < 1 ? Long.MAX_VALUE : retryDeadlineMillis;
            return this;
        }

        /**
         * Builds the retry config.
         * @return RetryConfig instance
         */
        public RetryConfig build() {
            return new RetryConfig(this);
        }
    }
}
