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

import io.nats.client.api.ConsumerConfiguration;

import static io.nats.client.support.Validator.*;

/**
 * The SubscribeOptions is the base class for PushSubscribeOptions and PullSubscribeOptions
 */
public abstract class SubscribeOptions {

    protected final String stream;
    protected final boolean pull;
    protected final boolean bind;
    protected final ConsumerConfiguration consumerConfig;
    protected final boolean detectGaps;
    protected final long expectedConsumerSeq;
    protected final long messageAlarmTime;

    @SuppressWarnings("rawtypes") // Don't need the type of the builder to get it's vars
    protected SubscribeOptions(Builder builder, boolean pull, String deliverSubject, String deliverGroup) {

        this.stream = validateStreamName(builder.stream, builder.bind); // required when bind mode

        String durable = validateMustMatchIfBothSupplied(builder.durable, builder.cc == null ? null : builder.cc.getDurable(), "Builder Durable", "Consumer Configuration Durable");
        durable = validateDurable(durable, pull || builder.bind); // required when pull or bind

        deliverGroup = validateMustMatchIfBothSupplied(deliverGroup, builder.cc == null ? null : builder.cc.getDeliverGroup(), "Builder Deliver Group", "Consumer Configuration Deliver Group");

        deliverSubject = validateMustMatchIfBothSupplied(deliverSubject, builder.cc == null ? null : builder.cc.getDeliverSubject(), "Builder Deliver Subject", "Consumer Configuration Deliver Subject");

        this.consumerConfig = ConsumerConfiguration.builder(builder.cc)
                .durable(durable)
                .deliverSubject(deliverSubject)
                .deliverGroup(deliverGroup)
                .build();

        this.pull = pull;
        this.bind = builder.bind;
        this.detectGaps = builder.detectGaps;
        this.expectedConsumerSeq = builder.expectedConsumerSeq;
        this.messageAlarmTime = builder.messageAlarmTime;
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
     * Gets whether this is a pull subscription
     * @return the pull flag
     */
    public boolean isPull() {
        return pull;
    }

    /**
     * Gets whether this subscription is expected to bind to an existing stream and durable consumer
     * @return the bind flag
     */
    public boolean isBind() {
        return bind;
    }

    /**
     * Get whether this subscription should provide automatic gap management,
     * i.e. handle when a gap is detected in the message stream.
     * @return the automatic gap management flag
     */
    public boolean detectGaps() {
        return detectGaps;
    }

    /**
     * Get the expected first expected sequence to use the first
     * time on auto gap detect
     * @return the expected sequence
     */
    public long getExpectedConsumerSeq() {
        return expectedConsumerSeq;
    }

    public long getMessageAlarmTime() {
        return messageAlarmTime;
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
        return getClass().getSimpleName() + "{" +
            "stream='" + stream + '\'' +
            "bind=" + bind +
            "detectGaps=" + detectGaps +
            ", " + consumerConfig +
            '}';
    }

    /**
     * PushSubscribeOptions can be created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     */
    protected static abstract class Builder<B, SO> {
        String stream;
        boolean bind;
        String durable;
        ConsumerConfiguration cc;
        boolean detectGaps = false;
        long expectedConsumerSeq = -1;
        long messageAlarmTime = -1;

        protected abstract B getThis();

        /**
         * Specify the stream to attach to. If not supplied the stream will be looked up by subject.
         * Null or empty clears the field.
         * @param stream the name of the stream
         * @return the builder
         */
        public B stream(String stream) {
            this.stream = emptyAsNull(stream);
            return getThis();
        }

        /**
         * Specify the to attach in direct mode
         * @return the builder
         * @param bind whether to bind or not
         */
        public B bind(boolean bind) {
            this.bind = bind;
            return getThis();
        }

        /**
         * Sets the durable consumer name for the subscriber.
         * Null or empty clears the field.
         * @param durable the durable name
         * @return the builder
         */
        public B durable(String durable) {
            this.durable = emptyAsNull(durable);
            return getThis();
        }

        /**
         * The consumer configuration. The configuration durable name will be replaced
         * if you supply a consumer name in the builder. The configuration deliver subject
         * will be replaced if you supply a name in the builder.
         * @param configuration the consumer configuration.
         * @return the builder
         */
        public B configuration(ConsumerConfiguration configuration) {
            this.cc = configuration;
            return getThis();
        }

        /**
         * Sets or clears the auto gap manage flag
         * @param detectGaps the flag
         * @return the builder
         */
        public B detectGaps(boolean detectGaps) {
            this.detectGaps = detectGaps;
            return getThis();
        }

        /**
         * The total amount of time to not receive any messages or heartbeats
         * before calling the ErrorListener heartbeatAlarm
         *
         * @param messageAlarmTime the time
         * @return the builder
         */
        public B messageAlarmTime(long messageAlarmTime) {
            this.messageAlarmTime = messageAlarmTime;
            return getThis();
        }

        /**
         * Sets the expected consumer sequence to use the first
         * time on auto gap detect. Set to &lt; 1 to allow
         * any first sequence
         * @param expectedConsumerSeq the consumer seq
         * @return the builder
         */
        public B expectedConsumerSeq(long expectedConsumerSeq) {
            this.expectedConsumerSeq = expectedConsumerSeq;
            return getThis();
        }

        /**
         * Builds the subscribe options.
         * @return subscribe options
         */
        public abstract SO build();
    }
}
