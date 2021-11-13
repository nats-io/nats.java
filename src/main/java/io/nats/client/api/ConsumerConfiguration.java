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

import io.nats.client.PullSubscribeOptions;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.support.ApiConstants;
import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonUtils;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.regex.Matcher;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;
import static io.nats.client.support.Validator.emptyAsNull;
import static io.nats.client.support.Validator.validateDurationNotRequiredNotLessThanMin;

/**
 * The ConsumerConfiguration class specifies the configuration for creating a JetStream consumer on the client and
 * if necessary the server.
 * Options are created using a PublishOptions.Builder.
 */
public class ConsumerConfiguration implements JsonSerializable {

    public static final Duration MIN_ACK_WAIT = Duration.ofNanos(1);
    public static final Duration MIN_IDLE_HEARTBEAT = Duration.ZERO;

    private final DeliverPolicy deliverPolicy;
    private final AckPolicy ackPolicy;
    private final ReplayPolicy replayPolicy;
    private final String description;
    private final String durable;
    private final String deliverSubject;
    private final String deliverGroup;
    private final String filterSubject;
    private final String sampleFrequency;
    private final ZonedDateTime startTime;
    private final Duration ackWait;
    private final Duration idleHeartbeat;
    private final Long startSeq;
    private final Long maxDeliver;
    private final Long rateLimit;
    private final Long maxAckPending;
    private final Long maxPullWaiting;
    private final Boolean flowControl;
    private final Boolean headersOnly;

    private static DeliverPolicy GetOrDefault(DeliverPolicy p) { return p == null ? DeliverPolicy.All : p; }
    private static AckPolicy GetOrDefault(AckPolicy p) { return p == null ? AckPolicy.Explicit : p; }
    private static ReplayPolicy GetOrDefault(ReplayPolicy p) { return p == null ? ReplayPolicy.Instant : p; }

    // for the response from the server
    ConsumerConfiguration(String json) {
        Matcher m = DELIVER_POLICY_RE.matcher(json);
        deliverPolicy = m.find() ? GetOrDefault(DeliverPolicy.get(m.group(1))) : GetOrDefault((DeliverPolicy)null);

        m = ACK_POLICY_RE.matcher(json);
        ackPolicy = m.find() ? GetOrDefault(AckPolicy.get(m.group(1))) : GetOrDefault((AckPolicy)null);

        m = REPLAY_POLICY_RE.matcher(json);
        replayPolicy = m.find() ? GetOrDefault(ReplayPolicy.get(m.group(1))) : GetOrDefault((ReplayPolicy)null);

        description = JsonUtils.readString(json, DESCRIPTION_RE);
        durable = JsonUtils.readString(json, DURABLE_NAME_RE);
        deliverSubject = JsonUtils.readString(json, DELIVER_SUBJECT_RE);
        deliverGroup = JsonUtils.readString(json, DELIVER_GROUP_RE);
        filterSubject = JsonUtils.readString(json, FILTER_SUBJECT_RE);
        sampleFrequency = JsonUtils.readString(json, SAMPLE_FREQ_RE);

        startTime = JsonUtils.readDate(json, OPT_START_TIME_RE);
        ackWait = JsonUtils.readNanos(json, ACK_WAIT_RE, (Duration)null);
        idleHeartbeat = JsonUtils.readNanos(json, IDLE_HEARTBEAT_RE, (Duration)null);

        startSeq = JsonUtils.readLong(json, OPT_START_SEQ_RE, CcNumeric.START_SEQ.initial());
        maxDeliver = JsonUtils.readLong(json, MAX_DELIVER_RE, CcNumeric.MAX_DELIVER.initial());
        rateLimit = JsonUtils.readLong(json, RATE_LIMIT_BPS_RE, CcNumeric.RATE_LIMIT.initial());
        maxAckPending = JsonUtils.readLong(json, MAX_ACK_PENDING_RE, CcNumeric.MAX_ACK_PENDING.initial());
        maxPullWaiting = JsonUtils.readLong(json, MAX_WAITING_RE, CcNumeric.MAX_PULL_WAITING.initial());
        flowControl = JsonUtils.readBoolean(json, FLOW_CONTROL_RE);
        headersOnly = JsonUtils.readBoolean(json, HEADERS_ONLY_RE);
    }

    // For the builder
    private ConsumerConfiguration(Builder b)
    {
        this.deliverPolicy = b.deliverPolicy;
        this.ackPolicy = b.ackPolicy;
        this.replayPolicy = b.replayPolicy;

        this.description = b.description;
        this.durable = b.durable;
        this.startTime = b.startTime;
        this.ackWait = b.ackWait;
        this.filterSubject = b.filterSubject;
        this.sampleFrequency = b.sampleFrequency;
        this.deliverSubject = b.deliverSubject;
        this.deliverGroup = b.deliverGroup;
        this.idleHeartbeat = b.idleHeartbeat;
        this.flowControl = b.flowControl;
        this.headersOnly = b.headersOnly;

        this.startSeq = b.startSeq;
        this.maxDeliver = b.maxDeliver;
        this.rateLimit = b.rateLimit;
        this.maxAckPending = b.maxAckPending;
        this.maxPullWaiting = b.maxPullWaiting;
    }

    /**
     * Returns a JSON representation of this consumer configuration.
     *
     * @return json consumer configuration json string
     */
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, DESCRIPTION, description);
        JsonUtils.addField(sb, DURABLE_NAME, durable);
        JsonUtils.addField(sb, DELIVER_SUBJECT, deliverSubject);
        JsonUtils.addField(sb, DELIVER_GROUP, deliverGroup);
        JsonUtils.addField(sb, DELIVER_POLICY, GetOrDefault(deliverPolicy).toString());
        JsonUtils.addField(sb, OPT_START_SEQ, startSeq);
        JsonUtils.addField(sb, OPT_START_TIME, startTime);
        JsonUtils.addField(sb, ACK_POLICY, GetOrDefault(ackPolicy).toString());
        JsonUtils.addFieldAsNanos(sb, ACK_WAIT, ackWait);
        JsonUtils.addField(sb, MAX_DELIVER, maxDeliver);
        JsonUtils.addField(sb, MAX_ACK_PENDING, maxAckPending);
        JsonUtils.addField(sb, FILTER_SUBJECT, filterSubject);
        JsonUtils.addField(sb, REPLAY_POLICY, GetOrDefault(replayPolicy).toString());
        JsonUtils.addField(sb, SAMPLE_FREQ, sampleFrequency);
        JsonUtils.addField(sb, RATE_LIMIT_BPS, rateLimit);
        JsonUtils.addFieldAsNanos(sb, IDLE_HEARTBEAT, idleHeartbeat);
        JsonUtils.addFldWhenTrue(sb, FLOW_CONTROL, flowControl);
        JsonUtils.addFldWhenTrue(sb, HEADERS_ONLY, headersOnly);
        JsonUtils.addField(sb, ApiConstants.MAX_WAITING, maxPullWaiting);
        return endJson(sb).toString();
    }

    /**
     * Gets the name of the description of this consumer configuration.
     * @return name of the description.
     */
    public String getDescription() {
        return description;
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
     * Gets the deliver group of this consumer configuration.
     * @return the deliver group.
     */
    public String getDeliverGroup() {
        return deliverGroup;
    }

    /**
     * Gets the deliver policy of this consumer configuration.
     * @return the deliver policy.
     */
    public DeliverPolicy getDeliverPolicy() {
        return GetOrDefault(deliverPolicy);
    }

    /**
     * Gets the start sequence of this consumer configuration.
     * @return the start sequence.
     */
    public long getStartSequence() {
        return CcNumeric.START_SEQ.valueOrInitial(startSeq);
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
        return GetOrDefault(ackPolicy);
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
        return CcNumeric.MAX_DELIVER.valueOrInitial(maxDeliver);
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
        return GetOrDefault(replayPolicy);
    }

    /**
     * Gets the rate limit for this consumer configuration.
     * @return the rate limit in msgs per second.
     */
    public long getRateLimit() {
        return CcNumeric.RATE_LIMIT.valueOrInitial(rateLimit);
    }

    /**
     * Gets the maximum ack pending configuration.
     * @return maximum ack pending.
     */
    public long getMaxAckPending() {
        return CcNumeric.MAX_ACK_PENDING.valueOrInitial(maxAckPending);
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
    public boolean isFlowControl() {
        return flowControl != null && flowControl;
    }

    /**
     * Get the number of pulls that can be outstanding on a pull consumer
     * @return the max pull waiting
     */
    public long getMaxPullWaiting() {
        return CcNumeric.MAX_PULL_WAITING.valueOrInitial(maxPullWaiting);
    }

    /**
     * Get the header only flag indicating whether it's on or off
     * @return the flow control mode
     */
    public boolean getHeadersOnly() {
        return headersOnly != null && headersOnly;
    }

    /**
     * Gets whether deliver policy of this consumer configuration was set or left unset
     * @return true if the policy was set, false if the policy was not set
     */
    public boolean deliverPolicyWasSet() {
        return deliverPolicy != null;
    }

    /**
     * Gets whether ack policy for this consumer configuration was set or left unset
     * @return true if the policy was set, false if the policy was not set
     */
    public boolean ackPolicyWasSet() {
        return ackPolicy != null;
    }

    /**
     * Gets whether replay policy for this consumer configuration was set or left unset
     * @return true if the policy was set, false if the policy was not set
     */
    public boolean replayPolicyWasSet() {
        return replayPolicy != null;
    }

    /**
     * Gets whether start sequence for this consumer configuration was set or left unset
     * @return true if the policy was set, false if the policy was not set
     */
    public boolean startSeqWasSet() {
        return startSeq != null;
    }

    /**
     * Gets whether max deliver for this consumer configuration was set or left unset
     * @return true if the policy was set, false if the policy was not set
     */
    public boolean maxDeliverWasSet() {
        return maxDeliver != null;
    }

    /**
     * Gets whether rate limit for this consumer configuration was set or left unset
     * @return true if the policy was set, false if the policy was not set
     */
    public boolean rateLimitWasSet() {
        return rateLimit != null;
    }

    /**
     * Gets whether max ack pending for this consumer configuration was set or left unset
     * @return true if the policy was set, false if the policy was not set
     */
    public boolean maxAckPendingWasSet() {
        return maxAckPending != null;
    }

    /**
     * Gets whether max pull waiting for this consumer configuration was set or left unset
     * @return true if the policy was set, false if the policy was not set
     */
    public boolean maxPullWaitingWasSet() {
        return maxPullWaiting != null;
    }

    /**
     * Gets whether flow control for this consumer configuration was set or left unset
     * @return true if the policy was set, false if the policy was not set
     */
    public boolean flowControlWasSet() {
        return flowControl != null;
    }

    /**
     * Gets whether headers only for this consumer configuration was set or left unset
     * @return true if the policy was set, false if the policy was not set
     */
    public boolean headersOnlyWasSet() {
        return headersOnly != null;
    }

    public boolean wouldBeChangeTo(ConsumerConfiguration original) {
        return (deliverPolicy != null && deliverPolicy != original.deliverPolicy)
            || (ackPolicy != null && ackPolicy != original.ackPolicy)
            || (replayPolicy != null && replayPolicy != original.replayPolicy)

            || (flowControl != null && flowControl != original.flowControl)
            || (headersOnly != null && headersOnly != original.headersOnly)

            || CcNumeric.START_SEQ.wouldBeChange(startSeq, original.startSeq)
            || CcNumeric.MAX_DELIVER.wouldBeChange(maxDeliver, original.maxDeliver)
            || CcNumeric.RATE_LIMIT.wouldBeChange(rateLimit, original.rateLimit)
            || CcNumeric.MAX_ACK_PENDING.wouldBeChange(maxAckPending, original.maxAckPending)
            || CcNumeric.MAX_PULL_WAITING.wouldBeChange(maxPullWaiting, original.maxPullWaiting)

            || (ackWait != null && !ackWait.equals(original.ackWait))
            || (idleHeartbeat != null && !idleHeartbeat.equals(original.idleHeartbeat))
            || (startTime != null && !startTime.equals(original.startTime))

            || (filterSubject != null && !filterSubject.equals(original.filterSubject))
            || (description != null && !description.equals(original.description))
            || (sampleFrequency != null && !sampleFrequency.equals(original.sampleFrequency))
            || (deliverSubject != null && !deliverSubject.equals(original.deliverSubject))
            || (deliverGroup != null && !deliverGroup.equals(original.deliverGroup))
            ;

        // do not need to check Durable because the original is retrieved by the durable name
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
        private DeliverPolicy deliverPolicy;
        private AckPolicy ackPolicy;
        private ReplayPolicy replayPolicy;

        private String description;
        private String durable;
        private String deliverSubject;
        private String deliverGroup;
        private String filterSubject;
        private String sampleFrequency;

        private ZonedDateTime startTime;
        private Duration ackWait;
        private Duration idleHeartbeat;

        private Long startSeq;
        private Long maxDeliver;
        private Long rateLimit;
        private Long maxAckPending;
        private Long maxPullWaiting;
        private Boolean flowControl;
        private Boolean headersOnly;

        public Builder() {}

        public Builder(ConsumerConfiguration cc) {
            if (cc != null) {
                this.deliverPolicy = cc.deliverPolicy;
                this.ackPolicy = cc.ackPolicy;
                this.replayPolicy = cc.replayPolicy;

                this.description = cc.description;
                this.durable = cc.durable;
                this.deliverSubject = cc.deliverSubject;
                this.deliverGroup = cc.deliverGroup;
                this.filterSubject = cc.filterSubject;
                this.sampleFrequency = cc.sampleFrequency;

                this.startTime = cc.startTime;
                this.ackWait = cc.ackWait;
                this.idleHeartbeat = cc.idleHeartbeat;

                this.startSeq = cc.startSeq;
                this.maxDeliver = cc.maxDeliver;
                this.rateLimit = cc.rateLimit;
                this.maxAckPending = cc.maxAckPending;
                this.maxPullWaiting = cc.maxPullWaiting;
                this.flowControl = cc.flowControl;
                this.headersOnly = cc.headersOnly;
            }
        }

        /**
         * Sets the description
         * @param description the description
         * @return the builder
         */
        public Builder description(String description) {
            this.description = emptyAsNull(description);
            return this;
        }

        /**
         * Sets the name of the durable subscription.
         * @param durable name of the durable subscription.
         * @return the builder
         */
        public Builder durable(String durable) {
            this.durable = emptyAsNull(durable);
            return this;
        }

        /**
         * Sets the delivery policy of the ConsumerConfiguration.
         * @param policy the delivery policy.
         * @return Builder
         */
        public Builder deliverPolicy(DeliverPolicy policy) {
            this.deliverPolicy = policy;
            return this;
        }

        /**
         * Sets the subject to deliver messages to.
         * @param subject the delivery subject.
         * @return the builder
         */
        public Builder deliverSubject(String subject) {
            this.deliverSubject = emptyAsNull(subject);
            return this;
        }

        /**
         * Sets the group to deliver messages to.
         * @param group the delivery group.
         * @return the builder
         */
        public Builder deliverGroup(String group) {
            this.deliverGroup = emptyAsNull(group);
            return this;
        }

        /**
         * Sets the start sequence of the ConsumerConfiguration or null to unset / clear.
         * @param sequence the start sequence
         * @return Builder
         */
        public Builder startSequence(Long sequence) {
            this.startSeq = sequence;
            return this;
        }

        /**
         * Sets the start sequence of the ConsumerConfiguration.
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
            this.ackPolicy = policy;
            return this;
        }

        /**
         * Sets the acknowledgement wait duration of the ConsumerConfiguration.
         * @param timeout the wait timeout
         * @return Builder
         */
        public Builder ackWait(Duration timeout) {
            this.ackWait = validateDurationNotRequiredNotLessThanMin(timeout, MIN_ACK_WAIT);
            return this;
        }

        /**
         * Sets the acknowledgement wait duration of the ConsumerConfiguration.
         * @param timeoutMillis the wait timeout in milliseconds
         * @return Builder
         */
        public Builder ackWait(long timeoutMillis) {
            this.ackWait = validateDurationNotRequiredNotLessThanMin(timeoutMillis, MIN_ACK_WAIT);
            return this;
        }

        /**
         * Sets the maximum delivery amount of the ConsumerConfiguration or null to unset / clear.
         * @param maxDeliver the maximum delivery amount
         * @return Builder
         */
        public Builder maxDeliver(Long maxDeliver) {
            this.maxDeliver = maxDeliver;
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
            this.filterSubject = emptyAsNull(filterSubject);
            return this;
        }

        /**
         * Sets the replay policy of the ConsumerConfiguration.
         * @param policy the replay policy.
         * @return Builder
         */
        public Builder replayPolicy(ReplayPolicy policy) {
            this.replayPolicy = policy;
            return this;
        }

        /**
         * Sets the sample frequency of the ConsumerConfiguration.
         * @param frequency the frequency
         * @return Builder
         */
        public Builder sampleFrequency(String frequency) {
            this.sampleFrequency = emptyAsNull(frequency);
            return this;
        }

        /**
         * Set the rate limit of the ConsumerConfiguration or null to unset / clear.
         * @param msgsPerSecond messages per second to deliver
         * @return Builder
         */
        public Builder rateLimit(Long msgsPerSecond) {
            this.rateLimit = msgsPerSecond;
            return this;
        }

        /**
         * Set the rate limit of the ConsumerConfiguration.
         * @param msgsPerSecond messages per second to deliver
         * @return Builder
         */
        public Builder rateLimit(long msgsPerSecond) {
            this.rateLimit = msgsPerSecond;
            return this;
        }

        /**
         * Sets the maximum ack pending or null to unset / clear.
         * @param maxAckPending maximum pending acknowledgements.
         * @return Builder
         */
        public Builder maxAckPending(Long maxAckPending) {
            this.maxAckPending = maxAckPending;
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
            this.idleHeartbeat = validateDurationNotRequiredNotLessThanMin(idleHeartbeat, MIN_IDLE_HEARTBEAT);
            return this;
        }

        /**
         * sets the idle heart beat wait time
         * @param idleHeartbeatMillis the idle heart beat duration in milliseconds
         * @return Builder
         */
        public Builder idleHeartbeat(long idleHeartbeatMillis) {
            this.idleHeartbeat = validateDurationNotRequiredNotLessThanMin(idleHeartbeatMillis, MIN_IDLE_HEARTBEAT);
            return this;
        }

        /**
         * set the flow control on and set the idle heartbeat
         * @param idleHeartbeat the idle heart beat duration
         * @return Builder
         */
        public Builder flowControl(Duration idleHeartbeat) {
            this.flowControl = true;
            return idleHeartbeat(idleHeartbeat);
        }

        /**
         * set the flow control on and set the idle heartbeat
         * @param idleHeartbeatMillis the idle heart beat duration in milliseconds
         * @return Builder
         */
        public Builder flowControl(long idleHeartbeatMillis) {
            this.flowControl = true;
            return idleHeartbeat(idleHeartbeatMillis);
        }

        /**
         * sets the max pull waiting, the number of pulls that can be outstanding on a pull consumer, pulls received after this is reached are ignored.
         * Use null to unset / clear.
         * @param maxPullWaiting the max pull waiting
         * @return Builder
         */
        public Builder maxPullWaiting(Long maxPullWaiting) {
            this.maxPullWaiting = maxPullWaiting;
            return this;
        }

        /**
         * sets the max pull waiting, the number of pulls that can be outstanding on a pull consumer, pulls received after this is reached are ignored.
         * @param maxPullWaiting the max pull waiting
         * @return Builder
         */
        public Builder maxPullWaiting(long maxPullWaiting) {
            this.maxPullWaiting = maxPullWaiting;
            return this;
        }

        /**
         * set the headers only flag
         * @param headersOnly the flag
         * @return Builder
         */
        public Builder headersOnly(Boolean headersOnly) {
            this.headersOnly = headersOnly;
            return this;
        }

        /**
         * Builds the ConsumerConfiguration
         * @return The consumer configuration.
         */
        public ConsumerConfiguration build() {
            return new ConsumerConfiguration(this);
        }

        /**
         * Builds the PushSubscribeOptions with this configuration
         * @return The PushSubscribeOptions.
         */
        public PushSubscribeOptions buildPushSubscribeOptions() {
            return PushSubscribeOptions.builder().configuration(build()).build();
        }

        /**
         * Builds the PullSubscribeOptions with this configuration
         * @return The PullSubscribeOptions.
         */
        public PullSubscribeOptions buildPullSubscribeOptions() {
            return PullSubscribeOptions.builder().configuration(build()).build();
        }
    }

    @Override
    public String toString() {
        return "ConsumerConfiguration{" +
            "description='" + description + '\'' +
            ", durable='" + durable + '\'' +
            ", deliverPolicy=" + deliverPolicy +
            ", deliverSubject='" + deliverSubject + '\'' +
            ", deliverGroup='" + deliverGroup + '\'' +
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
            ", maxPullWaiting=" + maxPullWaiting +
            ", headersOnly=" + headersOnly +
            '}';
    }

    public enum CcNumeric {
        START_SEQ(1, -1, -1),
        MAX_DELIVER(1, -1, -1),
        RATE_LIMIT(1, -1, -1),
        MAX_ACK_PENDING(0, 0, 20000L),
        MAX_PULL_WAITING(0, 0, 512);

        long min;
        long initial;
        long server;

        CcNumeric(long min, long initial, long server) {
            this.min = min;
            this.initial = initial;
            this.server = server;
        }

        public long initial() {
            return initial;
        }

        long valueOrInitial(Long val) {
            return val == null ? initial : val;
        }

        public long comparable(Long val) {
            return val == null || val < min || val == server ? initial : val;
        }

        public boolean wouldBeChange(Long user, Long srvr) {
            return user != null && comparable(user) != comparable(srvr);
        }
    }
}
