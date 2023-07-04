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

import java.time.ZonedDateTime;

import static io.nats.client.support.MiscUtils.generateConsumerName;
import static io.nats.client.support.Validator.emptyOrNullAs;

public class OrderedConsumerConfig {
    private final ConsumerConfiguration cc;

    private OrderedConsumerConfig(ConsumerConfiguration cc) {
        this.cc = cc;
    }

    /**
     * Macro to start a OrderedConsumerConfig builder
     * @return builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * OrderedConsumerConfig can be created using a Builder.
     * The builder supports chaining and will create a default set of options if
     * no methods are calls, including setting the filter subject to "&gt;"
     */
    public static class Builder {
        final ConsumerConfiguration.Builder builder;

        public Builder() {
            builder = new ConsumerConfiguration.Builder()
                .name(generateConsumerName())
                .filterSubject(">");
        }

        /**
         * Sets the delivery policy of the OrderedConsumerConfig.
         *
         * @param policy the delivery policy.
         * @return Builder
         */
        public Builder deliverPolicy(DeliverPolicy policy) {
            builder.deliverPolicy(policy);
            return this;
        }

        /**
         * Sets the start sequence of the OrderedConsumerConfig.
         *
         * @param sequence the start sequence
         * @return Builder
         */
        public Builder startSequence(long sequence) {
            builder.startSequence(sequence);
            return this;
        }

        /**
         * Sets the start time of the OrderedConsumerConfig.
         * @param startTime the start time
         * @return Builder
         */
        public Builder startTime(ZonedDateTime startTime) {
            builder.startTime(startTime);
            return this;
        }

        /**
         * Sets the filter subject of the OrderedConsumerConfig.
         *
         * @param filterSubject the filter subject
         * @return Builder
         */
        public Builder filterSubject(String filterSubject) {
            builder.filterSubject(emptyOrNullAs(filterSubject, ">"));
            return this;
        }

        /**
         * Sets the replay policy of the OrderedConsumerConfig.
         *
         * @param policy the replay policy.
         * @return Builder
         */
        public Builder replayPolicy(ReplayPolicy policy) {
            builder.replayPolicy(policy);
            return this;
        }

        /**
         * set the headers only flag saying to deliver only the headers of
         * messages in the stream and not the bodies
         *
         * @param headersOnly the flag
         * @return Builder
         */
        public Builder headersOnly(Boolean headersOnly) {
            builder.headersOnly(headersOnly);
            return this;
        }

        public OrderedConsumerConfig build() {
            return new OrderedConsumerConfig(builder.build());
        }
    }

    public ConsumerConfiguration getBackingConsumerConfiguration() {
        return cc;
    }
}
