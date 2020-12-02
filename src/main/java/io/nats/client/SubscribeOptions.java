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

import javax.jws.soap.SOAPBinding;

/**
 * The SubscribeOptions class specifies the options for subscribing with jetstream enabled servers.
 * Options are created using the constructor or a {@link SubscribeOptions.Builder Builder}.
 */
public class SubscribeOptions {

    private String stream = null;
    private String consumer = null;
    private ConsumerConfiguration consumerConfiguration = null;
    private boolean autoAck = true;
    private long pull = 0;

    private SubscribeOptions() {}

    /**
     * Gets the name of the stream.
     * @return the name of the stream.
     */
    public String getStream() {
        return stream;
    }

    /**
     * Sets the auto ack mode of this subscriber.  It is off by default.  When disabled
     * the application must manually ack using one of the message's Ack methods.
     * @param value true enables auto ack, false disables.
     */
    public void setAutoAck(boolean value) {
        autoAck = value;
    }

    /** 
     * Gets automatic message acknowedgement for this subscriber.
     * @return true is auto ack is enabled, false otherwise.
     */
    public boolean isAutoAck() {
        return autoAck;
    }


    /**
     * Sets the consumer configuration.
     * @param configuration consumer configuration.
     */
    public void setConfiguration(ConsumerConfiguration configuration) {
        if (configuration == null) {
            throw new IllegalArgumentException("Consumer configuration cannot be null");
        }
        this.consumerConfiguration = configuration;
    }

    /**
     * Gets the consumer configuration.
     * @return the consumer configuration.
     */
    public ConsumerConfiguration getConsumerConfiguration() {
        return consumerConfiguration;
    }

    /**
     * Sets the durable for subscription options.
     * @param durable the durable name
     */
    public void setDurable(String durable) {
        consumerConfiguration.setDurable(durable);
    }

    /**
     * Gets the attached consumer name.
     * @return the consumer name.
     */
    public String getConsumer() {
        return consumer;
    }

    public long getPullBatchSize() {
        return pull;
    }


    private static void checkStreamName(String stream) {
        if (stream == null || stream.length() == 0 || stream.contains(">") ||
        stream.contains(".") || stream.contains("*")) {
            throw new IllegalArgumentException("stream cannot be null, empty, tokenized, or wildcarded");
        }
    }
    /**
     * Setting this enables pull based consumers.  This sets the batch size the server
     * will send.  Default is 0 (disabled).
     * @param batchSize number of messages to request in poll. 
     */
    public void setPull(long batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Batch size must be greater than zero");
        }
        pull = batchSize;
    }

    /**
     * Setting this enables pull based consumers.  This sets the batch size the server
     * will send.  Default is 0 (disabled).
     * @param stream the name of the stream
     * @param consumer the name of the consumer
     * @param batchSize number of messages to request in poll. 
     */
    public void setPullDirect(String stream, String consumer, long batchSize) {
        checkStreamName(stream);
        setPull(batchSize);
        this.stream = stream;
        this.consumer = consumer;
    }    

    /**
     * Attaches to a consumer. 
     * @param stream the name of the stream the consumer is attached to.
     * @param consumer the consumer name.
     */
    public void attach(String stream, String consumer) {
        checkStreamName(stream);
        if (consumer == null) {
            throw new IllegalArgumentException("Consumer cannot be null");
        }
        this.stream = stream;
        this.consumer = consumer;
    }

    /**
     * Sets the delivery subject to push messages to.
     * @param deliverSubject the delivery subject.
     */
    public void pushDirect(String deliverSubject) {
        this.consumerConfiguration.setDeliverSubject(deliverSubject);
    }
    /**
     * Creates a builder for the publish options.
     * @return the builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * SubscribeOptions can be created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     */
    public static class Builder {
        private String stream = null;
        private String consumer = null;
        private ConsumerConfiguration consumerConfig = null;
        private boolean autoAck = true;
        private String durable = null;
        private long pull = 0;
        
        /**
         * Attaches to a consumer for the subscription.
         * @param stream the name of the stream
         * @param configuration the consumer configuration.
         * @return the builder
         */
        public Builder configuration(String stream, ConsumerConfiguration configuration) {
            this.stream = stream;
            this.consumerConfig = configuration;
            return this;
        }

        /**
         * Sets the durable name for the subscriber.
         * @param name the durable name
         * @return the builder
         */
        public Builder durable(String name) {
            this.durable = name;
            return this;
        }

        /**
         * Sets the auto ack mode of this subscriber.  It is off by default.  When disabled
         * the application must manually ack using one of the message's Ack methods.
         * @param value true enables auto ack, false disables.
         * @return the builder.
         */
        public Builder autoAck(boolean value) {
            this.autoAck = value;
            return this;
        }

        /**
         * Setting this enables pull based consumers.  This sets the batch size the server
         * will send.  Default is 0 (disabled).
         * @param batchSize number of messages to request in poll. 
         * @return the builder.
         */
        public Builder pull(long batchSize) {
            if (batchSize <= 0) {
                throw new IllegalArgumentException("Batch size must be greater than zero");
            }
            this.pull = batchSize;
            return this;
        }

        /**
         * Setting this enables pull based consumers.  This sets the batch size the server
         * will send.  Default is 0 (disabled).
         * @param stream the name of the stream
         * @param consumer the name of the consumer
         * @param batchSize number of messages to request in poll. 
         * @return the builder.
         */
        public Builder pullDirect(String stream, String consumer, long batchSize) {
            this.stream = stream;
            this.consumer = consumer;
            this.pull = batchSize;
            return this;
        }

        /**
         * Setting this specfies the push model to a delivery subject.
         * @param deliverSubject the subject to deliver on.
         * @return the builder.
         */
        public Builder pushDirect(String deliverSubject) {
            this.consumerConfig.setDeliverSubject(deliverSubject);
            return this;
        }          

        /**
         * Attaches to an existing consumer.
         * @param stream the stream name.
         * @param consumer the consumer name.
         * @return the builder.
         */
        public Builder attach(String stream, String consumer) {
            this.consumer = consumer;
            this.stream = stream;
            return this;
        }

        /**
         * Builds the subscribe options.
         * @return subscribe options
         */
        public SubscribeOptions build() {
            if (consumerConfig == null) {
                consumerConfig = ConsumerConfiguration.builder().build();
            }
            SubscribeOptions so = new SubscribeOptions();
            so.setAutoAck(autoAck);
            so.setConfiguration(consumerConfig);
            so.setDurable(durable);
            so.pull = pull;
            so.stream = stream;
            so.consumer = consumer;
            return so;

        }
    }
}
