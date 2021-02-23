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

import static io.nats.client.support.Validator.validateDurableRequired;
import static io.nats.client.support.Validator.validateStreamNameOrEmptyAsNull;

/**
 * The SubscribeOptions class specifies the options for subscribing with JetStream enabled servers.
 * Options are created using the constructors or a {@link Builder}.
 */
public class PullSubscribeOptions {

    public enum AckMode {ACK, NEXT}
    public enum ExpireMode {ADVANCE, LEAVE}

    private final String stream;
    private final ConsumerConfiguration consumerConfig;
    private final AckMode ackMode;
    private final ExpireMode expireMode;

    private PullSubscribeOptions(String stream, ConsumerConfiguration consumerConfig, AckMode ackMode, ExpireMode expireMode) {
        this.stream = stream;
        this.consumerConfig = consumerConfig;
        this.ackMode = ackMode;
        this.expireMode = expireMode;
    }

    /**
     * Gets the name of the stream.
     * @return the name of the stream.
     */
    public String getStream() {
        return stream;
    }

    public AckMode getAckMode() {
        return ackMode;
    }

    public ExpireMode getExpireMode() {
        return expireMode;
    }

    /**
     * Gets the durable consumer name held in the consumer configuration.
     * @return the durable consumer name
     */
    public String getDurable() {
        return consumerConfig.getDurable();
    }

    /**
     * Gets the consumer configuration.
     * @return the consumer configuration.
     */
    public ConsumerConfiguration getConsumerConfiguration() {
        return consumerConfig;
    }

    @Override
    public String toString() {
        return "PullSubscribeOptions{" +
                "stream='" + stream + '\'' +
                ", " + consumerConfig +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * SubscribeOptions can be created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     */
    public static class Builder {
        private String stream;
        private String durable;
        private AckMode ackMode;
        private ExpireMode expireMode;
        private ConsumerConfiguration consumerConfig;

        /**
         * Specify the stream to attach to. If not supplied the stream will be looked up by subject.
         * Null or empty clears the field.
         * @param stream the name of the stream
         * @return the builder
         */
        public Builder stream(String stream) {
            this.stream = stream;
            return this;
        }

        /**
         * Sets the durable consumer name for the subscriber.
         * @param durable the durable name
         * @return the builder
         */
        public Builder durable(String durable) {
            this.durable = durable;
            return this;
        }

        public Builder ackMode(AckMode ackMode) {
            this.ackMode = ackMode;
            return this;
        }

        public Builder expireMode(ExpireMode expireMode) {
            this.expireMode = expireMode;
            return this;
        }

        /**
         * The consumer configuration. The configuration durable name will be replaced
         * if you supply a consumer name in the builder. The configuration deliver subject
         * will be replaced if you supply a name in the builder.
         * @param configuration the consumer configuration.
         * @return the builder
         */
        public Builder configuration(ConsumerConfiguration configuration) {
            this.consumerConfig = configuration;
            return this;
        }

        /**
         * Builds the subscribe options.
         * @return subscribe options
         */
        public PullSubscribeOptions build() {
            validateStreamNameOrEmptyAsNull(stream);
            durable = validateDurableRequired(durable, consumerConfig);
            ConsumerConfiguration cc = ConsumerConfiguration.builder(consumerConfig).durable(durable).build();
            return new PullSubscribeOptions(stream, cc,
                    ackMode == null ? AckMode.ACK : ackMode,
                    expireMode == null ? ExpireMode.ADVANCE : expireMode);
        }
    }
}
