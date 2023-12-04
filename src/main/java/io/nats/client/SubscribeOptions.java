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

import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;

import java.time.Duration;

import static io.nats.client.support.NatsJetStreamClientError.*;
import static io.nats.client.support.Validator.*;

/**
 * The SubscribeOptions is the base class for PushSubscribeOptions and PullSubscribeOptions
 */
public abstract class SubscribeOptions {
    public static final long DEFAULT_ORDERED_HEARTBEAT = 5000;

    protected final String stream;
    protected final boolean pull;
    protected final boolean bind;
    protected final boolean fastBind;
    protected final boolean ordered;
    protected final long messageAlarmTime;
    protected final ConsumerConfiguration consumerConfig;
    protected final long pendingMessageLimit; // Only applicable for non dispatched (sync) push consumers.
    protected final long pendingByteLimit; // Only applicable for non dispatched (sync) push consumers.
    protected final String name;
    protected Deserializer deserializer;

    @SuppressWarnings("rawtypes") // Don't need the type of the builder to get its vars
    protected SubscribeOptions(Builder builder, boolean isPull,
                               String deliverSubject, String deliverGroup,
                               long pendingMessageLimit, long pendingByteLimit) {

        pull = isPull;
        fastBind = builder.fastBind;
        bind = fastBind || builder.bind;
        ordered = builder.ordered;
        messageAlarmTime = builder.messageAlarmTime;

        if (ordered && bind) {
            throw JsSoOrderedNotAllowedWithBind.instance();
        }

        stream = validateStreamName(builder.stream, bind); // required when bind mode

        // read the consumer names and do basic validation
        // A1. validate name input
        String ccName = validateMustMatchIfBothSupplied(
            builder.name,
            builder.cc == null ? null : builder.cc.getName(),
            JsSoNameMismatch);
        // B1. Must be a valid consumer name if supplied
        ccName = validateConsumerName(ccName, false);

        // A2. validate durable input
        String ccDurable = validateMustMatchIfBothSupplied(
            builder.durable,
            builder.cc == null ? null : builder.cc.getDurable(),
            JsSoDurableMismatch);

        // B2. Must be a valid consumer name if supplied
        ccDurable = validateDurable(ccDurable, false);

        // C. name must match durable if both supplied
        name = validateMustMatchIfBothSupplied(ccName, ccDurable, JsConsumerNameDurableMismatch);

        if (bind && name == null) {
            throw JsSoNameOrDurableRequiredForBind.instance();
        }

        deliverGroup = validateMustMatchIfBothSupplied(deliverGroup, builder.cc == null ? null : builder.cc.getDeliverGroup(), JsSoDeliverGroupMismatch);

        deliverSubject = validateMustMatchIfBothSupplied(deliverSubject, builder.cc == null ? null : builder.cc.getDeliverSubject(), JsSoDeliverSubjectMismatch);

        this.pendingMessageLimit = pendingMessageLimit;
        this.pendingByteLimit = pendingByteLimit;

        if (ordered) {
            validateNotSupplied(deliverGroup, JsSoOrderedNotAllowedWithDeliverGroup);
            validateNotSupplied(ccDurable, JsSoOrderedNotAllowedWithDurable);
            validateNotSupplied(deliverSubject, JsSoOrderedNotAllowedWithDeliverSubject);
            long hb = DEFAULT_ORDERED_HEARTBEAT;
            if (builder.cc != null) {
                // want to make sure they didn't set it or they didn't set it to something other than none
                if (builder.cc.ackPolicyWasSet() && builder.cc.getAckPolicy() != AckPolicy.None) {
                    throw JsSoOrderedRequiresAckPolicyNone.instance();
                }
                if (builder.cc.getMaxDeliver() > 1) {
                    throw JsSoOrderedRequiresMaxDeliverOfOne.instance();
                }
                if (builder.cc.memStorageWasSet() && !builder.cc.isMemStorage()) {
                    throw JsSoOrderedMemStorageNotSuppliedOrTrue.instance();
                }
                if (builder.cc.numReplicasWasSet() && builder.cc.getNumReplicas() != 1) {
                    throw JsSoOrderedReplicasNotSuppliedOrOne.instance();
                }
                if (!pull) {
                    Duration ccHb = builder.cc.getIdleHeartbeat();
                    if (ccHb != null) {
                        hb = ccHb.toMillis();
                    }
                }
            }
            ConsumerConfiguration.Builder b = ConsumerConfiguration.builder(builder.cc)
                .ackPolicy(AckPolicy.None)
                .maxDeliver(1)
                .ackWait(Duration.ofHours(22))
                .name(ccName)
                .memStorage(true)
                .numReplicas(1);

            if (!pull) {
                b.flowControl(hb);
            }
            consumerConfig = b.build();
        }
        else {
            consumerConfig = ConsumerConfiguration.builder(builder.cc)
                .name(ccName)
                .durable(ccDurable)
                .deliverSubject(deliverSubject)
                .deliverGroup(deliverGroup)
                .build();
        }
        
        if(builder.deserializer != null) {
            this.deserializer = builder.deserializer;
        }
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
     * Gets the name of the consumer. Same as durable when the consumer is durable.
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Gets whether this is a pull subscription
     * @return the pull flag
     */
    public boolean isPull() {
        return pull;
    }

    /**
     * Gets whether this subscription is expected to bind to an existing stream and consumer
     * @return the bind flag
     */
    public boolean isBind() {
        return bind;
    }

    /**
     * Gets whether this subscription is expected to fast bind to an existing stream and consumer.
     * Overrides bind
     * @return the fast bind flag
     */
    public boolean isFastBind() {
        return fastBind;
    }

    /**
     * Gets whether this subscription is expected to ensure messages come in order
     * @return the ordered flag
     */
    public boolean isOrdered() {
        return ordered;
    }

    /**
     * Get the time amount of time allowed to elapse without a heartbeat.
     * If not set will default to 3 times the idle heartbeat setting
     * @return the message alarm time
     */
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

    /**
     * Gets the pending message limit. Only applicable for non dispatched (sync) push consumers.
     * @return the message limit
     */
    public long getPendingMessageLimit() {
        return pendingMessageLimit;
    }

    /**
     * Gets the pending byte limit. Only applicable for non dispatched (sync) push consumers.
     * @return the byte limit
     */
    public long getPendingByteLimit() {
        return pendingByteLimit;
    }

    public Deserializer getDeserializer() { return deserializer; };

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
            "name='" + name + '\'' +
            ", stream='" + stream + '\'' +
            ", pull='" + pull + '\'' +
            ", bind=" + bind +
            ", fastBind=" + fastBind +
            ", ordered=" + ordered +
            ", messageAlarmTime=" + messageAlarmTime +
            ", " + consumerConfig +
            '}';
    }

    /**
     * PushSubscribeOptions can be created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     */
    protected static abstract class Builder<B, SO> {
        protected String stream;
        protected boolean bind;
        protected boolean fastBind;
        protected String durable;
        protected String name;
        protected ConsumerConfiguration cc;
        protected long messageAlarmTime = -1;
        protected boolean ordered;
        protected Deserializer deserializer;


        protected abstract B getThis();

        /**
         * Specify the stream to attach to. If not supplied the stream will be looked up by subject.
         * Null or empty clears the field.
         * @param stream the name of the stream
         * @return the builder
         */
        public B stream(String stream) {
            this.stream = validateStreamName(stream, false);
            return getThis();
        }

        /**
         * Specify binding to an existing consumer via name.
         * The client validates regular (non-fast)
         * binds to ensure that provided consumer configuration
         * is consistent with the server version and that
         * consumer type (push versus pull) matches the subscription type.
         * @return the builder
         * @param bind whether to bind or not
         */
        public B bind(boolean bind) {
            this.bind = bind;
            return getThis();
        }
        /**
         * Sets the durable name for the consumer.
         * Null or empty clears the field.
         * @param durable the durable name
         * @return the builder
         */
        public B durable(String durable) {
            this.durable = validateDurable(durable, false);
            return getThis();
        }

        /**
         * Sets the name of the consumer.
         * Null or empty clears the field.
         * @param name name of the consumer.
         * @return the builder
         */
        public B name(String name) {
            this.name = validateConsumerName(name, false);
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
         * Set the total amount of time to not receive any messages or heartbeats
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
         * Builds the subscribe options.
         * @return subscribe options
         */
        public abstract SO build();

        public B deserializer(Deserializer deserializer) {
            this.deserializer = deserializer;
            return getThis();
        }
    }
}
