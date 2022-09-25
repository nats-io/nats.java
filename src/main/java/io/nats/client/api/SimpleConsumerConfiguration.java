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

import java.time.Duration;
import java.time.ZonedDateTime;

import static io.nats.client.support.Validator.nullOrEmpty;

/**
 * The SimpleConsumerConfiguration class specifies the configuration for creating a JetStream2 consumer.
 * Options are created using a SimpleConsumerConfiguration.Builder.
 */
public class SimpleConsumerConfiguration extends ConsumerConfiguration {

    protected SimpleConsumerConfiguration(ConsumerConfiguration cc) {
        super(cc);
    }

    /**
     * Creates a builder for the options.
     * @return a publish options builder
     */
    public static Builder simpleBuilder() {
        return new Builder();
    }

    /**
     * Creates a builder for the options.
     * @param cc the consumer configuration
     * @return a publish options builder
     */
    public static Builder simpleBuilder(SimpleConsumerConfiguration cc) {
        return cc == null ? new Builder() : new Builder(cc);
    }

    /**
     * SimpleConsumerConfiguration is created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     *
     * <p>{@code new SimpleConsumerConfiguration.simpleBuilder().build()} will create a default ConsumerConfiguration.
     *
     */
    public static class Builder {
        ConsumerConfiguration.Builder builder;

        public Builder() {
            builder = new ConsumerConfiguration.Builder();
        }

        public Builder(SimpleConsumerConfiguration cc) {
            if (cc != null) {
                builder = new ConsumerConfiguration.Builder(cc);
            }
            else {
                builder = new ConsumerConfiguration.Builder();
            }
        }

        public Builder(ConsumerConfiguration cc) {
            if (cc != null) {
                if (!nullOrEmpty(cc.getDurable())) {
                    throw new IllegalArgumentException("Consumer cannot be durable.");
                }
                if (!nullOrEmpty(cc.getDeliverSubject())) {
                    throw new IllegalArgumentException("Consumer cannot have a deliver subject.");
                }
                builder = new ConsumerConfiguration.Builder(cc);
            }
            else {
                builder = new ConsumerConfiguration.Builder();
            }
        }

        /**
         * Sets the description
         * @param description the description
         * @return the builder
         */
        public Builder description(String description) {
            builder.description(description);
            return this;
        }

        /**
         * Sets the name of the durable consumer.
         * Null or empty clears the field.
         * @param durable name of the durable consumer.
         * @return the builder
         */
        public Builder durable(String durable) {
            builder.durable(durable);
            return this;
        }

        /**
         * Sets the name of the consumer.
         * Null or empty clears the field.
         * @param name name of the consumer.
         * @return the builder
         */
        public Builder name(String name) {
            builder.name(name);
            return this;
        }

        /**
         * Sets the delivery policy of the ConsumerConfiguration.
         * @param policy the delivery policy.
         * @return Builder
         */
        public Builder deliverPolicy(DeliverPolicy policy) {
            builder.deliverPolicy(policy);
            return this;
        }

        /**
         * Sets the start sequence of the ConsumerConfiguration or null to unset / clear.
         * @param sequence the start sequence
         * @return Builder
         */
        public Builder startSequence(Long sequence) {
            builder.startSequence(sequence);
            return this;
        }

        /**
         * Sets the start sequence of the ConsumerConfiguration.
         * @param sequence the start sequence
         * @return Builder
         */
        public Builder startSequence(long sequence) {
            builder.startSequence(sequence);
            return this;
        }

        /**
         * Sets the start time of the ConsumerConfiguration.
         * @param startTime the start time
         * @return Builder
         */
        public Builder startTime(ZonedDateTime startTime) {
            builder.startTime(startTime);
            return this;
        }

        /**
         * Sets the acknowledgement policy of the ConsumerConfiguration.
         * @param policy the acknowledgement policy.
         * @return Builder
         */
        public Builder ackPolicy(AckPolicy policy) {
            builder.ackPolicy(policy);
            return this;
        }

        /**
         * Sets the acknowledgement wait duration of the ConsumerConfiguration.
         * @param timeout the wait timeout
         * @return Builder
         */
        public Builder ackWait(Duration timeout) {
            builder.ackWait(timeout);
            return this;
        }

        /**
         * Sets the acknowledgement wait duration of the ConsumerConfiguration.
         * @param timeoutMillis the wait timeout in milliseconds
         * @return Builder
         */
        public Builder ackWait(long timeoutMillis) {
            builder.ackWait(timeoutMillis);
            return this;
        }

        /**
         * Sets the maximum delivery amount of the ConsumerConfiguration or null to unset / clear.
         * @param maxDeliver the maximum delivery amount
         * @return Builder
         */
        public Builder maxDeliver(Long maxDeliver) {
            builder.maxDeliver(maxDeliver);
            return this;
        }

        /**
         * Sets the maximum delivery amount of the ConsumerConfiguration.
         * @param maxDeliver the maximum delivery amount
         * @return Builder
         */
        public Builder maxDeliver(long maxDeliver) {
            builder.maxDeliver(maxDeliver);
            return this;
        }

        /**
         * Sets the filter subject of the ConsumerConfiguration.
         * @param filterSubject the filter subject
         * @return Builder
         */
        public Builder filterSubject(String filterSubject) {
            builder.filterSubject(filterSubject);
            return this;
        }

        /**
         * Sets the replay policy of the ConsumerConfiguration.
         * @param policy the replay policy.
         * @return Builder
         */
        public Builder replayPolicy(ReplayPolicy policy) {
            builder.replayPolicy(policy);
            return this;
        }

        /**
         * Sets the maximum ack pending or null to unset / clear.
         * @param maxAckPending maximum pending acknowledgements.
         * @return Builder
         */
        public Builder maxAckPending(Long maxAckPending) {
            builder.maxAckPending(maxAckPending);
            return this;
        }

        /**
         * Sets the maximum ack pending.
         * @param maxAckPending maximum pending acknowledgements.
         * @return Builder
         */
        public Builder maxAckPending(long maxAckPending) {
            builder.maxAckPending(maxAckPending);
            return this;
        }

        /**
         * sets the max amount of expire time for the server to allow on pull requests.
         * @param maxExpires the max expire duration
         * @return Builder
         */
        public Builder maxExpires(Duration maxExpires) {
            builder.maxExpires(maxExpires);
            return this;
        }

        /**
         * sets the max amount of expire time for the server to allow on pull requests.
         * @param maxExpires the max expire duration in milliseconds
         * @return Builder
         */
        public Builder maxExpires(long maxExpires) {
            builder.maxExpires(maxExpires);
            return this;
        }

        /**
         * sets the amount of time before the ephemeral consumer is deemed inactive.
         * @param inactiveThreshold the threshold duration
         * @return Builder
         */
        public Builder inactiveThreshold(Duration inactiveThreshold) {
            builder.inactiveThreshold(inactiveThreshold);
            return this;
        }

        /**
         * sets the amount of time before the ephemeral consumer is deemed inactive.
         * @param inactiveThreshold the threshold duration in milliseconds
         * @return Builder
         */
        public Builder inactiveThreshold(long inactiveThreshold) {
            builder.inactiveThreshold(inactiveThreshold);
            return this;
        }

        /**
         * sets the max pull waiting, the number of pulls that can be outstanding on a pull consumer, pulls received after this is reached are ignored.
         * Use null to unset / clear.
         * @param maxPullWaiting the max pull waiting
         * @return Builder
         */
        public Builder maxPullWaiting(Long maxPullWaiting) {
            builder.maxPullWaiting(maxPullWaiting);
            return this;
        }

        /**
         * sets the max pull waiting, the number of pulls that can be outstanding on a pull consumer, pulls received after this is reached are ignored.
         * @param maxPullWaiting the max pull waiting
         * @return Builder
         */
        public Builder maxPullWaiting(long maxPullWaiting) {
            builder.maxPullWaiting(maxPullWaiting);
            return this;
        }

        /**
         * sets the max batch size for the server to allow on pull requests.
         * @param maxBatch the max batch size
         * @return Builder
         */
        public Builder maxBatch(Long maxBatch) {
            builder.maxBatch(maxBatch);
            return this;
        }

        /**
         * sets the max batch size for the server to allow on pull requests.
         * @param maxBatch the max batch size
         * @return Builder
         */
        public Builder maxBatch(long maxBatch) {
            builder.maxBatch(maxBatch);
            return this;
        }

        /**
         * sets the max bytes size for the server to allow on pull requests.
         * @param maxBytes the max bytes size
         * @return Builder
         */
        public Builder maxBytes(Long maxBytes) {
            builder.maxBytes(maxBytes);
            return this;
        }

        /**
         * sets the max bytes size for the server to allow on pull requests.
         * @param maxBytes the max bytes size
         * @return Builder
         */
        public Builder maxBytes(long maxBytes) {
            builder.maxBytes(maxBytes);
            return this;
        }

        /**
         * set the number of replicas for the consumer. When set do not inherit the
         * replica count from the stream but specifically set it to this amount.
         * @param numReplicas number of replicas for the consumer
         * @return Builder
         */
        public Builder numReplicas(Integer numReplicas) {
            builder.numReplicas(numReplicas);
            return this;
        }

        /**
         * set the headers only flag saying to deliver only the headers of
         * messages in the stream and not the bodies
         * @param headersOnly the flag
         * @return Builder
         */
        public Builder headersOnly(Boolean headersOnly) {
            builder.headersOnly(headersOnly);
            return this;
        }

        /**
         * set the mem storage flag to force the consumer state to be kept
         * in memory rather than inherit the setting from the stream
         * @param memStorage the flag
         * @return Builder
         */
        public Builder memStorage(Boolean memStorage) {
            builder.memStorage(memStorage);
            return this;
        }

        /**
         * Set the list of backoff.
         * @param backoffs zero or more backoff durations or an array of backoffs
         * @return Builder
         */
        public Builder backoff(Duration... backoffs) {
            builder.backoff(backoffs);
            return this;
        }

        /**
         * Set the list of backoff.
         * @param backoffsMillis zero or more backoff in millis or an array of backoffsMillis
         * @return Builder
         */
        public Builder backoff(long... backoffsMillis) {
            builder.backoff(backoffsMillis);
            return this;
        }

        /**
         * Builds the ConsumerConfiguration
         * @return The consumer configuration.
         */
        public SimpleConsumerConfiguration build() {
            return new SimpleConsumerConfiguration(builder.build());
        }
    }
}
