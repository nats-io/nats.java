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

package io.nats.client;

import java.time.Duration;

/**
 * The PublishOptions class specifies the options for publishing with JetStream enabled servers.
 * Options are created using a {@link ForceReconnectOptions.Builder Builder}.
 */
public class ForceReconnectOptions {

    /**
     * A default instance of ForceReconnectOptions
     */
    public static final ForceReconnectOptions DEFAULT_INSTANCE = ForceReconnectOptions.builder().build();

    /**
     * An instance representing the force close option
     */
    public static final ForceReconnectOptions FORCE_CLOSE_INSTANCE = ForceReconnectOptions.builder().forceClose().build();

    private final boolean forceClose;
    private final Duration flushWait;

    private ForceReconnectOptions(Builder b) {
        this.forceClose = b.forceClose;
        this.flushWait = b.flushWait;
    }

    /**
     * True if these options represent force close
     * @return the flag
     */
    public boolean isForceClose() {
        return forceClose;
    }

    /**
     * True if these options represent to flush
     * @return the flag
     */
    public boolean isFlush() {
        return flushWait != null;
    }

    /**
     * Get the flush wait setting
     * @return the duration
     */
    public Duration getFlushWait() {
        return flushWait;
    }

    /**
     * Creates a builder for the options.
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * ForceReconnectOptions are created using a Builder.
     */
    public static class Builder {
        boolean forceClose = false;
        Duration flushWait;

        /**
         * Constructs a new Builder with the default values.
         */
        public Builder() {}

        /**
         * set the force close flag to true
         * @return the builder
         */
        public Builder forceClose() {
            this.forceClose = true;
            return this;
        }

        /**
         * if supplied and at least 1 millisecond, the forceReconnect will try to
         * flush before closing for the specified wait time. Flush happens before close
         * so not affected by forceClose option
         * @param flushWait the flush wait duraton
         * @return the builder
         */
        public Builder flush(Duration flushWait) {
            this.flushWait = flushWait == null || flushWait.toMillis() < 1 ? null : flushWait;
            return this;
        }

        /**
         * if supplied and at least 1 millisecond, the forceReconnect will try to
         * flush before closing for the specified wait time. Flush happens before close
         * so not affected by forceClose option
         * @param flushWaitMillis the flush wait millis
         * @return the builder
         */
        public Builder flush(long flushWaitMillis) {
            if (flushWaitMillis > 0) {
                this.flushWait = Duration.ofMillis(flushWaitMillis);
            }
            else {
                this.flushWait = null;
            }
            return this;
        }

        /**
         * Builds the ForceReconnectOptions.
         * @return ForceReconnectOptions
         */
        public ForceReconnectOptions build() {
            return new ForceReconnectOptions(this);
        }
    }
}
