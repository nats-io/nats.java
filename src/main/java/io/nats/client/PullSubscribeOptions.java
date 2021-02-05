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
public class PullSubscribeOptions {

    private final String stream;
    private final int batchSize;
    private final boolean noWait;
    private final ConsumerConfiguration consumerConfig;

    public PullSubscribeOptions(String stream, int batchSize, boolean noWait, ConsumerConfiguration consumerConfig) {
        this.stream = stream;
        this.batchSize = batchSize;
        this.noWait = noWait;
        this.consumerConfig = consumerConfig;
    }

    /**
     * Gets the name of the stream.
     * @return the name of the stream.
     */
    public String getStream() {
        return stream;
    }

    /**
     * Gets the pull batch size
     * @return the size
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Gets the durable consumer name held in the consumer configuration.
     * @return the durable consumer name
     */
    public String getDurable() {
        return consumerConfig.getDurable();
    }

    /**
     * When true a response with a 404 status header will be returned when no messages are available
     * @return the flag
     */
    public boolean isNoWait() {
        return noWait;
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
                ", batchSize=" + batchSize +
                ", noWait=" + noWait +
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
        private int batchSize;
        private boolean noWait;
        private String durable;
        private ConsumerConfiguration consumerConfig;

        /**
         * Specify the stream to attach to. If not supplied the stream will be looked up by subject.
         * Null or empty clears the field.
         * @param stream the name of the stream
         * @return the builder
         */
        public Builder stream(String stream) {
            this.stream = validateStreamNameOrEmptyAsNull(stream);
            return this;
        }

        /**
         * Sets the pull batch size.
         * @param batchSize the size
         * @return the builder
         */
        public Builder batchSize(int batchSize) {
            this.batchSize = validatePullBatchSize(batchSize);
            return this;
        }

        /**
         * When true a response with a 404 status header will be returned when no messages are available
         * @param noWait true to turn on this behavior
         */
        public void setNoWait(boolean noWait) {
            this.noWait = noWait;
        }

        /**
         * Sets the durable consumer name for the subscriber.
         * @param durable the durable name
         * @return the builder
         */
        public Builder durable(String durable) {
            this.durable = validateDurableRequired(durable);
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

            validatePullBatchSize(batchSize);

            this.consumerConfig = (consumerConfig == null)
                    ? ConsumerConfiguration.defaultConfiguration()
                    : consumerConfig;

            if (durable != null) {
                consumerConfig.setDurable(durable);
            }
            else {
                validateDurableRequired(consumerConfig.getDurable());
            }

            return new PullSubscribeOptions(stream, batchSize, noWait, consumerConfig);
        }
    }
}
