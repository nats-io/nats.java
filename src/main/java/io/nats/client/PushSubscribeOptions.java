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

import static io.nats.client.support.Validator.*;

/**
 * The SubscribeOptions class specifies the options for subscribing with JetStream enabled servers.
 * Options are created using the constructors or a {@link Builder}.
 */
public class PushSubscribeOptions {

    private final String stream;
    private final ConsumerConfiguration consumerConfig;

    private PushSubscribeOptions() {
        this(null, null, null, null);
    }

    private PushSubscribeOptions(String stream, String durable, String deliverSubject,
                                 ConsumerConfiguration consumerConfig) {

        this.stream = stream;
        this.consumerConfig = ConsumerConfiguration.builder(consumerConfig)
                .durable(durable)
                .deliverSubject(deliverSubject)
                .build();
    }

    /**
     * Gets the name of the stream.
     * @return the name of the stream.
     */
    public String getStream() {
        return stream;
    }

    /**
     * Gets the durable consumer name held in the consumer configuration.
     * @return the durable consumer name
     */
    public String getDurable() {
        return consumerConfig.getDurable();
    }

    /**
     * Gets the deliver subject held in the consumer configuration.
     * @return the Deliver subject
     */
    public String getDeliverSubject() {
        return consumerConfig.getDeliverSubject();
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
        return "PushSubscribeOptions{" +
                "stream='" + stream + '\'' +
                ", " + consumerConfig +
                '}';
    }

    /**
     * Get a default instance of SubscribeOptions
     * @return the instance
     */
    public static PushSubscribeOptions defaultInstance() {
        return new PushSubscribeOptions();
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
        private String deliverSubject;
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
         * Null or empty clears the field.
         * @param durable the durable name
         * @return the builder
         */
        public Builder durable(String durable) {
            this.durable = durable;
            return this;
        }

        /**
         * Setting this specifies the push model to a delivery subject.
         * Null or empty clears the field.
         * @param deliverSubject the subject to deliver on.
         * @return the builder.
         */
        public Builder deliverSubject(String deliverSubject) {
            this.deliverSubject = deliverSubject;
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
        public PushSubscribeOptions build() {
            validateStreamNameOrEmptyAsNull(stream);
            validateDurableOrEmptyAsNull(durable);
            return new PushSubscribeOptions(stream, durable, emptyAsNull(deliverSubject), consumerConfig);
        }
    }
}
