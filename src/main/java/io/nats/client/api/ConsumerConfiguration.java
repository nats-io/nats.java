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

import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonUtils;
import io.nats.client.support.Validator;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.regex.Matcher;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;

/**
 * The ConsumerConfiguration class specifies the configuration for creating a JetStream consumer on the client and
 * if necessary the server.
 * Options are created using a PublishOptions.Builder.
 */
public class ConsumerConfiguration implements JsonSerializable {

    private static final long MIN_ACK_WAIT_MILLIS = 1;
    private static final long MIN_IDLE_HEARTBEAT_MILLIS = 0;
    private static final Duration DEFAULT_ACK_WAIT = Duration.ofSeconds(30);
    private static final Duration DEFAULT_IDLE_HEARTBEAT = Duration.ZERO;

    private final DeliverPolicy deliverPolicy;
    private final AckPolicy ackPolicy;
    private final ReplayPolicy replayPolicy;

    private final String durable;
    private final String deliverSubject;
    private final long startSeq;
    private final ZonedDateTime startTime;
    private final Duration ackWait;
    private final long maxDeliver;
    private final String filterSubject;
    private final String sampleFrequency;
    private final long rateLimit;
    private final long maxAckPending;
    private final Duration idleHeartbeat;
    private final boolean flowControl;

    // for the response from the server
    ConsumerConfiguration(String json) {

        Matcher m = DELIVER_POLICY_RE.matcher(json);
        deliverPolicy = m.find() ? DeliverPolicy.get(m.group(1)) : DeliverPolicy.All;

        m = ACK_POLICY_RE.matcher(json);
        ackPolicy = m.find() ? AckPolicy.get(m.group(1)) : AckPolicy.Explicit;

        m = REPLAY_POLICY_RE.matcher(json);
        replayPolicy = m.find() ? ReplayPolicy.get(m.group(1)) : ReplayPolicy.Instant;

        durable = JsonUtils.readString(json, DURABLE_NAME_RE);
        deliverSubject = JsonUtils.readString(json, DELIVER_SUBJECT_RE);
        startSeq = JsonUtils.readUnsignedLong(json, OPT_START_SEQ_RE, 0);
        startTime = JsonUtils.readDate(json, OPT_START_TIME_RE);
        ackWait = JsonUtils.readNanos(json, ACK_WAIT_RE, Duration.ofSeconds(30));
        maxDeliver = JsonUtils.readLong(json, MAX_DELIVER_RE, -1);
        filterSubject = JsonUtils.readString(json, FILTER_SUBJECT_RE);
        sampleFrequency = JsonUtils.readString(json, SAMPLE_FREQ_RE);
        rateLimit = JsonUtils.readLong(json, RATE_LIMIT_BPS_RE, 0);
        maxAckPending = JsonUtils.readLong(json, MAX_ACK_PENDING_RE, 0);
        idleHeartbeat = JsonUtils.readNanos(json, IDLE_HEARTBEAT_RE, Duration.ZERO);
        flowControl = JsonUtils.readBoolean(json, FLOW_CONTROL_RE);
    }

    // For the builder
    private ConsumerConfiguration(String durable, DeliverPolicy deliverPolicy, long startSeq,
            ZonedDateTime startTime, AckPolicy ackPolicy, Duration ackWait, long maxDeliver, String filterSubject,
            ReplayPolicy replayPolicy, String sampleFrequency, long rateLimit, String deliverSubject, long maxAckPending,
            Duration idleHeartbeat, boolean flowControl) {
                this.durable = durable;
                this.deliverPolicy = deliverPolicy;
                this.startSeq = startSeq;
                this.startTime = startTime;
                this.ackPolicy = ackPolicy;
                this.ackWait = ackWait;
                this.maxDeliver = maxDeliver;
                this.filterSubject = filterSubject;
                this.replayPolicy = replayPolicy;
                this.sampleFrequency = sampleFrequency;
                this.rateLimit = rateLimit;
                this.deliverSubject = deliverSubject;
                this.maxAckPending = maxAckPending;
                this.idleHeartbeat = idleHeartbeat;
                this.flowControl = flowControl;
    }

    /**
     * Returns a JSON representation of this consumer configuration.
     * 
     * @return json consumer configuration json string
     */
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, DURABLE_NAME, durable);
        JsonUtils.addField(sb, DELIVER_SUBJECT, deliverSubject);
        JsonUtils.addField(sb, DELIVER_POLICY, deliverPolicy.toString());
        JsonUtils.addFieldAsUnsigned(sb, OPT_START_SEQ, startSeq);
        JsonUtils.addField(sb, OPT_START_TIME, startTime);
        JsonUtils.addField(sb, ACK_POLICY, ackPolicy.toString());
        JsonUtils.addFieldAsNanos(sb, ACK_WAIT, ackWait);
        JsonUtils.addField(sb, MAX_DELIVER, maxDeliver);
        JsonUtils.addField(sb, MAX_ACK_PENDING, maxAckPending);
        JsonUtils.addField(sb, FILTER_SUBJECT, filterSubject);
        JsonUtils.addField(sb, REPLAY_POLICY, replayPolicy.toString());
        JsonUtils.addField(sb, SAMPLE_FREQ, sampleFrequency);
        JsonUtils.addField(sb, RATE_LIMIT_BPS, rateLimit);
        JsonUtils.addFieldAsNanos(sb, IDLE_HEARTBEAT, idleHeartbeat);
        JsonUtils.addField(sb, FLOW_CONTROL, flowControl);
        return endJson(sb).toString();
    }

    /**
     * Gets the name of the durable subscription for this consumer configuration.
     * @return name of the durable.
     */
    public String getDurable() {
        return durable;
    }

    /**
     * Gets the deliver subject of this consumer configuration.
     * @return the deliver subject.
     */    
    public String getDeliverSubject() {
        return deliverSubject;
    }

    /**
     * Gets the deliver policy of this consumer configuration.
     * @return the deliver policy.
     */    
    public DeliverPolicy getDeliverPolicy() {
        return deliverPolicy;
    }

    /**
     * Gets the start sequence of this consumer configuration.
     * @return the start sequence.
     */    
    public long getStartSequence() {
        return startSeq;
    }

    /**
     * Gets the start time of this consumer configuration.
     * @return the start time.
     */    
    public ZonedDateTime getStartTime() {
        return startTime;
    }

    /**
     * Gets the acknowledgment policy of this consumer configuration.
     * @return the acknowledgment policy.
     */    
    public AckPolicy getAckPolicy() {
        return ackPolicy;
    }

    /**
     * Gets the acknowledgment wait of this consumer configuration.
     * @return the acknowledgment wait duration.
     */     
    public Duration getAckWait() {
        return ackWait;
    }

    /**
     * Gets the max delivery amount of this consumer configuration.
     * @return the max delivery amount.
     */      
    public long getMaxDeliver() {
        return maxDeliver;
    }

    /**
     * Gets the max filter subject of this consumer configuration.
     * @return the filter subject.
     */    
    public String getFilterSubject() {
        return filterSubject;
    }

    /**
     * Gets the replay policy of this consumer configuration.
     * @return the replay policy.
     */     
    public ReplayPolicy getReplayPolicy() {
        return replayPolicy;
    }

    /**
     * Gets the rate limit for this consumer configuration.
     * @return the rate limit in msgs per second.
     */      
    public long getRateLimit() {
        return rateLimit;
    }

    /**
     * Gets the maximum ack pending configuration.
     * @return maximum ack pending.
     */
    public long getMaxAckPending() {
        return maxAckPending;
    }

    /**
     * Gets the sample frequency.
     * @return sampleFrequency.
     */
    public String getSampleFrequency() {
        return sampleFrequency;
    }


    /**
     * Gets the idle heart beat wait time
     * @return the idle heart beat wait duration.
     */
    public Duration getIdleHeartbeat() {
        return idleHeartbeat;
    }

    /**
     * Get the flow control flag indicating whether it's on or off
     * @return the flow control mode
     */
    public boolean getFlowControl() {
        return flowControl;
    }

    /**
     * Creates a builder for the publish options.
     * @return a publish options builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a builder for the publish options.
     * @param cc the consumer configuration
     * @return a publish options builder
     */
    public static Builder builder(ConsumerConfiguration cc) {
        return cc == null ? new Builder() : new Builder(cc);
    }

    /**
     * ConsumerConfiguration is created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     * 
     * <p>{@code new ConsumerConfiguration.Builder().build()} will create a default ConsumerConfiguration.
     * 
     */
    public static class Builder {

        private String durable = null;
        private DeliverPolicy deliverPolicy = DeliverPolicy.All;
        private long startSeq = 0L;
        private ZonedDateTime startTime = null;
        private AckPolicy ackPolicy = AckPolicy.Explicit;
        private Duration ackWait = Duration.ofSeconds(30);
        private long maxDeliver = -1;
        private String filterSubject = null;
        private ReplayPolicy replayPolicy = ReplayPolicy.Instant;
        private String sampleFrequency = null;
        private long rateLimit = 0;
        private String deliverSubject = null;
        private long maxAckPending = 0;
        private Duration idleHeartbeat = Duration.ZERO;
        private boolean flowControl;

        public String getDurable() {
            return durable;
        }

        public String getDeliverSubject() {
            return deliverSubject;
        }

        public String getFilterSubject() {
            return filterSubject;
        }

        public long getMaxAckPending() {
            return maxAckPending;
        }

        public AckPolicy getAckPolicy() {
            return ackPolicy;
        }

        public Builder() {}

        public Builder(ConsumerConfiguration cc) {
            this.durable = cc.durable;
            this.deliverPolicy = cc.deliverPolicy;
            this.startSeq = cc.startSeq;
            this.startTime = cc.startTime;
            this.ackPolicy = cc.ackPolicy;
            this.ackWait = cc.ackWait;
            this.maxDeliver = cc.maxDeliver;
            this.filterSubject = cc.filterSubject;
            this.replayPolicy = cc.replayPolicy;
            this.sampleFrequency = cc.sampleFrequency;
            this.rateLimit = cc.rateLimit;
            this.deliverSubject = cc.deliverSubject;
            this.maxAckPending = cc.maxAckPending;
            this.idleHeartbeat = cc.idleHeartbeat;
            this.flowControl = cc.flowControl;
        }

        /**
         * Sets the name of the durable subscription.
         * @param durable name of the durable subscription.
         * @return the builder
         */
        public Builder durable(String durable) {
            this.durable = durable;
            return this;
        }      

        /**
         * Sets the delivery policy of the ConsumerConfiguration.
         * @param policy the delivery policy.
         * @return Builder
         */
        public Builder deliverPolicy(DeliverPolicy policy) {
            this.deliverPolicy = policy == null ? DeliverPolicy.All : policy;
            return this;
        }

        /**
         * Sets the subject to deliver messages to.
         * @param subject the delivery subject.
         * @return the builder
         */
        public Builder deliverSubject(String subject) {
            this.deliverSubject = subject;
            return this;
        }  

        /**
         * Sets the start sequence of the ConsumerConfiguration.
         * This ling will be treated as unsigned
         * @param sequence the start sequence
         * @return Builder
         */
        public Builder startSequence(long sequence) {
            this.startSeq = sequence;
            return this;
        }

        /**
         * Sets the start time of the ConsumerConfiguration.
         * @param startTime the start time
         * @return Builder
         */        
        public Builder startTime(ZonedDateTime startTime) {
            this.startTime = startTime;
            return this;
        }

        /**
         * Sets the acknowledgement policy of the ConsumerConfiguration.
         * @param policy the acknowledgement policy.
         * @return Builder
         */        
        public Builder ackPolicy(AckPolicy policy) {
            this.ackPolicy = policy == null ? AckPolicy.Explicit : policy;
            return this;
        }

        /**
         * Sets the acknowledgement wait duration of the ConsumerConfiguration.
         * @param timeout the wait timeout
         * @return Builder
         */
        public Builder ackWait(Duration timeout) {
            this.ackWait = Validator.ensureNotNullAndNotLessThanMin(timeout, DEFAULT_ACK_WAIT, MIN_ACK_WAIT_MILLIS);
            return this;
        }

        /**
         * Sets the acknowledgement wait duration of the ConsumerConfiguration.
         * @param timeoutMillis the wait timeout in milliseconds
         * @return Builder
         */
        public Builder ackWait(long timeoutMillis) {
            this.ackWait = Validator.ensureDurationNotLessThanMin(timeoutMillis, DEFAULT_ACK_WAIT, MIN_ACK_WAIT_MILLIS);
            return this;
        }

        /**
         * Sets the maximum delivery amount of the ConsumerConfiguration.
         * @param maxDeliver the maximum delivery amount
         * @return Builder
         */        
        public Builder maxDeliver(long maxDeliver) {
            this.maxDeliver = maxDeliver;
            return this;
        }

        /**
         * Sets the filter subject of the ConsumerConfiguration.
         * @param filterSubject the filter subject
         * @return Builder
         */         
        public Builder filterSubject(String filterSubject) {
            this.filterSubject = filterSubject;
            return this;
        }

        /**
         * Sets the replay policy of the ConsumerConfiguration.
         * @param policy the replay policy.
         * @return Builder
         */         
        public Builder replayPolicy(ReplayPolicy policy) {
            this.replayPolicy = policy == null ? ReplayPolicy.Instant : policy;
            return this;
        }

        /**
         * Sets the sample frequency of the ConsumerConfiguration.
         * @param frequency the frequency
         * @return Builder
         */
        public Builder sampleFrequency(String frequency) {
            this.sampleFrequency = frequency;
            return this;
        }

        /**
         * Set the rate limit of the ConsumerConfiguration.
         * @param msgsPerSecond messages per second to deliver
         * @return Builder
         */
        public Builder rateLimit(int msgsPerSecond) {
            this.rateLimit = msgsPerSecond;
            return this;
        }

        /**
         * Sets the maximum ack pending.
         * @param maxAckPending maximum pending acknowledgements.
         * @return Builder
         */
        public Builder maxAckPending(long maxAckPending) {
            this.maxAckPending = maxAckPending;
            return this;
        }

        /**
         * sets the idle heart beat wait time
         * @param idleHeartbeat the idle heart beat duration
         * @return Builder
         */
        public Builder idleHeartbeat(Duration idleHeartbeat) {
            this.idleHeartbeat = Validator.ensureNotNullAndNotLessThanMin(idleHeartbeat, Duration.ZERO, MIN_IDLE_HEARTBEAT_MILLIS);
            return this;
        }

        /**
         * sets the idle heart beat wait time
         * @param idleHeartbeatMillis the idle heart beat duration in milliseconds
         * @return Builder
         */
        public Builder idleHeartbeat(long idleHeartbeatMillis) {
            this.idleHeartbeat = Validator.ensureDurationNotLessThanMin(idleHeartbeatMillis, Duration.ZERO, MIN_IDLE_HEARTBEAT_MILLIS);
            return this;
        }

        /**
         * set the flow control mode
         * @param flowControl the flow control mode flag
         * @return Builder
         */
        public Builder flowControl(final boolean flowControl) {
            this.flowControl = flowControl;
            return this;
        }

        /**
         * Builds the ConsumerConfiguration
         * @return a consumer configuration.
         */
        public ConsumerConfiguration build() {

            return new ConsumerConfiguration(
                    durable,
                    deliverPolicy,
                    startSeq,
                    startTime,
                    ackPolicy,
                    ackWait,
                    maxDeliver,
                    filterSubject,
                    replayPolicy,
                    sampleFrequency,
                    rateLimit,
                    deliverSubject,
                    maxAckPending,
                    idleHeartbeat,
                    flowControl
            );
        }
    }

    @Override
    public String toString() {
        return "ConsumerConfiguration{" +
                "durable='" + durable + '\'' +
                ", deliverPolicy=" + deliverPolicy +
                ", deliverSubject='" + deliverSubject + '\'' +
                ", startSeq=" + startSeq +
                ", startTime=" + startTime +
                ", ackPolicy=" + ackPolicy +
                ", ackWait=" + ackWait +
                ", maxDeliver=" + maxDeliver +
                ", filterSubject='" + filterSubject + '\'' +
                ", replayPolicy=" + replayPolicy +
                ", sampleFrequency='" + sampleFrequency + '\'' +
                ", rateLimit=" + rateLimit +
                ", maxAckPending=" + maxAckPending +
                ", idleHeartbeat=" + idleHeartbeat +
                ", flowControl=" + flowControl +
                '}';
    }
}
