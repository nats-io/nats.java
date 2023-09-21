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

package io.nats.client.api;

import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonUtils;
import io.nats.client.support.JsonValue;

import java.time.Duration;

import static io.nats.client.api.ConsumerConfiguration.*;
import static io.nats.client.support.ApiConstants.INACTIVE_THRESHOLD;
import static io.nats.client.support.ApiConstants.MAX_ACK_PENDING;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;
import static io.nats.client.support.JsonValueUtils.readInteger;
import static io.nats.client.support.JsonValueUtils.readNanos;

/**
 * ConsumerLimits
 */
public class ConsumerLimits implements JsonSerializable {
    private final Duration inactiveThreshold;
    private final Integer maxAckPending;

    static ConsumerLimits optionalInstance(JsonValue vConsumerLimits) {
        return vConsumerLimits == null ? null : new ConsumerLimits(vConsumerLimits);
    }

    ConsumerLimits(JsonValue vConsumerLimits) {
        inactiveThreshold = readNanos(vConsumerLimits, INACTIVE_THRESHOLD);
        maxAckPending = readInteger(vConsumerLimits, MAX_ACK_PENDING);
    }

    ConsumerLimits(ConsumerLimits.Builder b) {
        this.inactiveThreshold = b.inactiveThreshold;
        this.maxAckPending = b.maxAckPending;
    }

    /**
     * Get the amount of time before the consumer is deemed inactive.
     * @return the inactive threshold
     */
    public Duration getInactiveThreshold() {
        return inactiveThreshold;
    }

    /**
     * Gets the maximum ack pending configuration.
     * @return maximum ack pending.
     */
    public long getMaxAckPending() {
        return getOrUnset(maxAckPending);
    }

    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addFieldAsNanos(sb, INACTIVE_THRESHOLD, inactiveThreshold);
        JsonUtils.addField(sb, MAX_ACK_PENDING, maxAckPending);
        return endJson(sb).toString();
    }

    /**
     * Creates a builder for a consumer limits object.
     * @return the builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * ConsumerLimits can be created using a Builder.
     */
    public static class Builder {
        private Duration inactiveThreshold;
        private Integer maxAckPending;

        /**
         * sets the amount of time before the consumer is deemed inactive.
         * @param inactiveThreshold the threshold duration
         * @return Builder
         */
        public Builder inactiveThreshold(Duration inactiveThreshold) {
            this.inactiveThreshold = normalize(inactiveThreshold);
            return this;
        }

        /**
         * sets the amount of time before the consumer is deemed inactive.
         * @param inactiveThreshold the threshold duration in milliseconds
         * @return Builder
         */
        public Builder inactiveThreshold(long inactiveThreshold) {
            this.inactiveThreshold = normalizeDuration(inactiveThreshold);
            return this;
        }

        /**
         * Sets the maximum ack pending or null to unset / clear.
         * @param maxAckPending maximum pending acknowledgements.
         * @return Builder
         */
        public Builder maxAckPending(Long maxAckPending) {
            this.maxAckPending = normalize(maxAckPending, STANDARD_MIN);
            return this;
        }

        /**
         * Sets the maximum ack pending.
         * @param maxAckPending maximum pending acknowledgements.
         * @return Builder
         */
        public Builder maxAckPending(long maxAckPending) {
            this.maxAckPending = normalize(maxAckPending, STANDARD_MIN);
            return this;
        }

        /**
         * Build a ConsumerLimits object
         * @return the ConsumerLimits
         */
        public ConsumerLimits build() {
            return new ConsumerLimits(this);
        }
    }
}
