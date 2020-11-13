// Copyright 2015-2018 The NATS Authors
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

/**
 * The PublishOptions class specifies the options for publishing with jetstream enabled servers.
 * Options are created using a {@link PublishOptions.Builder Builder}.
 */
public class SubscribeOptions {

    private String stream = null;
    private ConsumerConfiguration consumer = null;

    // TODO:  Add properties

    /**
     * Constructs a SubscribeOptions object with a stream and consumer configuration.
     * @param stream the name of the stream
     * @param consumerConfiguration the consumer configuration
     */
    public SubscribeOptions(String stream, ConsumerConfiguration consumerConfiguration) {
        if (consumerConfiguration == null) {
            throw new IllegalArgumentException("consumerConfiguration cannot be null");
        }
        this.consumer = consumerConfiguration;
        setStream(stream);
    }

    /**
     * Gets the name of the stream.
     * @return the name of the stream.
     */
    public String getStream() {
        return stream;
    }

    /**
     * Sets the name of the stream
     * @param stream the name fo the stream.
     */
    public void setStream(String stream) {
        if (stream == null || stream.length() == 0 || stream.contains(">") ||
            stream.contains(".") || stream.contains("*")) {
            throw new IllegalArgumentException("stream cannot be null, empty, tokenized, or wildcarded");
        }
        this.stream = stream;
    }

    /**
     * Gets the consumer configuration.
     * @return the consumer configuration.
     */
    public ConsumerConfiguration getConsumerConfiguration() {
        return consumer;
    }

    /**
     * Creates a builder for the publish options.
     * @return the builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * PublishOptions are created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls. The builder can also
     * be created from a properties object using the property names defined with the
     * prefix PROP_ in this class.
     */
    public static class Builder {
        private String stream = null;
        private ConsumerConfiguration consumer = null;
        
        /**
         * Sets the consumer configuration for the subscription.
         * @param consumerConfig the consumer configuration.
         * @return the builder
         */
        public Builder consumer(String stream, ConsumerConfiguration consumerConfig) {
            this.stream = stream;
            this.consumer = consumerConfig;
            return this;
        }

        /**
         * Builds the publish options.
         * @return subscribe options
         */
        public SubscribeOptions build() {
            return new SubscribeOptions(stream, consumer);
        }
    }

}
