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

package io.nats.client;

import java.time.Duration;

/**
 * The SimpleConsumerOptions class specifies the general options for JetStream.
 * Options are created using the  {@link SimpleConsumerOptions.Builder Builder}.
 */
public class SimpleConsumerOptions {
    public static final SimpleConsumerOptions DEFAULT_SCO_OPTIONS = builder().build();
    public static final SimpleConsumerOptions XLARGE_PAYLOAD = predefined(10, 6);
    public static final SimpleConsumerOptions LARGE_PAYLOAD = predefined(10, 6);
    public static final SimpleConsumerOptions MEDIUM_PAYLOAD = predefined(30, 18);
    public static final SimpleConsumerOptions SMALL_PAYLOAD = predefined(100, 60);

    public final int batchSize;
    public final int maxBytes;
    public final int repullAt;
    public final boolean ordered;

    public SimpleConsumerOptions(Builder b) {
        this.batchSize = b.batchSize;
        this.maxBytes = b.maxBytes;
        this.repullAt = b.repullAt;
        this.ordered = b.ordered;
    }

    /**
     * Creates a builder for the pull options, with batch size since it's always required
     * @return a pull options builder
     */
    public static Builder builder() {
        return new Builder();
    }

    private static SimpleConsumerOptions predefined(int batchSize, int repullAt) {
        return new Builder().batchSize(batchSize).repullAt(repullAt).build();
    }

    public static class Builder {
        private int batchSize;
        private int maxBytes;
        private int repullAt;
        private boolean ordered;
        private Duration expiresIn;
        private Duration idleHeartbeat;

        /**
         * Set the batch size for the pull
         * @param batchSize the size of the batch. Must be greater than 0
         * @return the builder
         */
        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize < 1 ? -1 : batchSize;
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
        public Builder repullAt(int repullAt) {
            this.repullAt = repullAt;
            return this;
        }

//        /**
//         * Set the expires time in millis
//         * @param expiresInMillis the millis
//         * @return the builder
//         */
//        public Builder expiresIn(long expiresInMillis) {
//            this.expiresIn = Duration.ofMillis(expiresInMillis);
//            return this;
//        }
//
//        /**
//         * Set the expires duration
//         * @param expiresIn the duration
//         * @return the builder
//         */
//        public Builder expiresIn(Duration expiresIn) {
//            this.expiresIn = expiresIn;
//            return this;
//        }
//
//        /**
//         * Set the idle heartbeat time in millis
//         * @param idleHeartbeatMillis the millis
//         * @return the builder
//         */
//        public Builder idleHeartbeat(long idleHeartbeatMillis) {
//            this.idleHeartbeat = Duration.ofMillis(idleHeartbeatMillis);
//            return this;
//        }
//
//        /**
//         * Set the idle heartbeat duration
//         * @param idleHeartbeat the duration
//         * @return the builder
//         */
//        public Builder idleHeartbeat(Duration idleHeartbeat) {
//            this.idleHeartbeat = idleHeartbeat;
//            return this;
//        }

        /**
         * Build the SimpleConsumerOptions. Validates that the batch size is greater than 0
         * @return the built PullRequestOptions
         */
        public SimpleConsumerOptions build() {
            // try to set some reasonable defaults. Are these right?
            if (batchSize < 1) {
                batchSize = 100;
            }
            if (maxBytes > 0) {
                if (repullAt < 1) {
                    repullAt = maxBytes * 50/100;
                }
                else if (repullAt > maxBytes) {
                    repullAt = maxBytes * 50/100;
                }
            }
            else if (repullAt < 1) {
                repullAt = batchSize * 50/100;
            }
            else if (repullAt > batchSize) {
                repullAt = maxBytes * 50/100;
            }
            return new SimpleConsumerOptions(this);
        }
    }
}
