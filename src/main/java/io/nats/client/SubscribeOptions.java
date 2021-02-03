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
public class SubscribeOptions {

    private final String stream;
    private final String durable;
    private final String deliverSubject;
    private final ConsumerConfiguration consumerConfig;

    private SubscribeOptions() {
        this(null, null, null, null);
    }

    private SubscribeOptions(String stream, String durable, String deliverSubject,
                             ConsumerConfiguration consumerConfig) {

        this.stream = stream;
        this.durable = durable;
        this.deliverSubject = deliverSubject;
        this.consumerConfig = (consumerConfig == null)
                ? ConsumerConfiguration.defaultConfiguration()
                : consumerConfig;
    }

    /**
     * Gets the name of the stream.
     * @return the name of the stream.
     */
    public String getStream() {
        return stream;
    }

    public String getDurable() {
        return durable;
    }

    public String getDeliverSubject() {
        return deliverSubject;
    }

    /**
     * Gets the consumer configuration.
     * @return the consumer configuration.
     */
    public ConsumerConfiguration getConsumerConfiguration() {
        return consumerConfig;
    }

    /**
     * Returns a copy of the subscribe options (for thread safety) or the default instance if
     * the supplied options are null
     * @param options null or an existing options to copy.
     * @return the new options.
     */
    public static SubscribeOptions createOrCopy(SubscribeOptions options) {
        if (options == null) {
            return new SubscribeOptions();
        }

        return new SubscribeOptions(options.stream, options.durable, options.deliverSubject, options.consumerConfig);
    }

    @Override
    public String toString() {
        return "SubscribeOptions{" +
                "stream='" + stream + '\'' +
                ", durable='" + durable + '\'' +
                ", deliverSubject='" + deliverSubject + '\'' +
                ", " + consumerConfig +
                '}';
    }

    /**
     * Get a default instance of SubscribeOptions
     * @return the instance
     */
    public static SubscribeOptions defaultInstance() {
        return new SubscribeOptions();
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * SubscribeOptions can be created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     */
    public static class Builder {
        protected String stream;
        protected String durable;
        protected String deliverSubject;
        protected ConsumerConfiguration consumerConfig;

        /**
         * Specify the stream to attach to. If not supplied the stream will be looked up by subject.
         * @param stream the name of the stream
         * @return the builder
         */
        public Builder stream(String stream) {
            this.stream = validateStreamName(stream);
            return this;
        }

        /**
         * Sets the durable consumer name for the subscriber.
         * @param durable the durable name
         * @return the builder
         */
        public Builder durable(String durable) {
            this.durable = validateConsumerRequired(durable);
            return this;
        }

        /**
         * Setting this specifies the push model to a delivery subject.
         * @param deliverSubject the subject to deliver on.
         * @return the builder.
         */
        public Builder deliverSubject(String deliverSubject) {
            this.deliverSubject = validateDeliverSubjectRequired(deliverSubject);
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
        public SubscribeOptions build() {
            return new SubscribeOptions(stream, durable, deliverSubject, consumerConfig);
        }
    }
}
