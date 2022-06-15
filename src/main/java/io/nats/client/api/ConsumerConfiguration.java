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
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;
import static io.nats.client.support.Validator.emptyAsNull;

/**
 * The ConsumerConfiguration class specifies the configuration for creating a JetStream consumer on the client and
 * if necessary the server.
 * Options are created using a PublishOptions.Builder.
 */
public class ConsumerConfiguration implements JsonSerializable {
    @Deprecated
    public static final Duration DURATION_MIN = Duration.ofNanos(1);

    public static final DeliverPolicy DEFAULT_DELIVER_POLICY = DeliverPolicy.All;
    public static final AckPolicy DEFAULT_ACK_POLICY = AckPolicy.Explicit;
    public static final ReplayPolicy DEFAULT_REPLAY_POLICY = ReplayPolicy.Instant;

    public static final Duration DURATION_UNSET = Duration.ZERO;
    public static final Duration MIN_IDLE_HEARTBEAT = Duration.ofMillis(100);

    public static final int INTEGER_UNSET = -1;
    public static final long LONG_UNSET = -1;
    public static final long ULONG_UNSET = 0;
    public static final long DURATION_UNSET_LONG = 0;
    public static final long DURATION_MIN_LONG = 1;
    public static final int STANDARD_MIN = 0;
    public static final int MAX_DELIVER_MIN = 1;

    public static final long MIN_IDLE_HEARTBEAT_NANOS = MIN_IDLE_HEARTBEAT.toNanos();
    public static final long MIN_IDLE_HEARTBEAT_MILLIS = MIN_IDLE_HEARTBEAT.toMillis();

    protected final DeliverPolicy deliverPolicy;
    protected final AckPolicy ackPolicy;
    protected final ReplayPolicy replayPolicy;
    protected final String description;
    protected final String durable;
    protected final String deliverSubject;
    protected final String deliverGroup;
    protected final String filterSubject;
    protected final String sampleFrequency;
    protected final ZonedDateTime startTime;
    protected final Duration ackWait;
    protected final Duration idleHeartbeat;
    protected final Duration maxExpires;
    protected final Duration inactiveThreshold;
    protected final Long startSeq; // server side this is unsigned
    protected final Integer maxDeliver;
    protected final Long rateLimit; // server side this is unsigned
    protected final Integer maxAckPending;
    protected final Integer maxPullWaiting;
    protected final Integer maxBatch;
    protected final Integer maxBytes;
    protected final Boolean flowControl;
    protected final Boolean headersOnly;
    protected final List<Duration> backoff;

    protected ConsumerConfiguration(ConsumerConfiguration cc) {
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
        this.maxExpires = cc.maxExpires;
        this.inactiveThreshold = cc.inactiveThreshold;
        this.startSeq = cc.startSeq;
        this.maxDeliver = cc.maxDeliver;
        this.rateLimit = cc.rateLimit;
        this.maxAckPending = cc.maxAckPending;
        this.maxPullWaiting = cc.maxPullWaiting;
        this.maxBatch = cc.maxBatch;
        this.maxBytes = cc.maxBytes;
        this.flowControl = cc.flowControl;
        this.headersOnly = cc.headersOnly;
        this.backoff = new ArrayList<>(cc.backoff);
    }

    // for the response from the server
    ConsumerConfiguration(String json) {
        Matcher m = DELIVER_POLICY_RE.matcher(json);
        deliverPolicy = m.find() ? DeliverPolicy.get(m.group(1)) : null;

        m = ACK_POLICY_RE.matcher(json);
        ackPolicy = m.find() ? AckPolicy.get(m.group(1)) : null;

        m = REPLAY_POLICY_RE.matcher(json);
        replayPolicy = m.find() ? ReplayPolicy.get(m.group(1)) : null;

        description = JsonUtils.readString(json, DESCRIPTION_RE);
        durable = JsonUtils.readString(json, DURABLE_NAME_RE);
        deliverSubject = JsonUtils.readString(json, DELIVER_SUBJECT_RE);
        deliverGroup = JsonUtils.readString(json, DELIVER_GROUP_RE);
        filterSubject = JsonUtils.readString(json, FILTER_SUBJECT_RE);
        sampleFrequency = JsonUtils.readString(json, SAMPLE_FREQ_RE);

        startTime = JsonUtils.readDate(json, OPT_START_TIME_RE);
        ackWait = JsonUtils.readNanos(json, ACK_WAIT_RE);
        idleHeartbeat = JsonUtils.readNanos(json, IDLE_HEARTBEAT_RE);
        maxExpires = JsonUtils.readNanos(json, MAX_EXPIRES_RE);
        inactiveThreshold = JsonUtils.readNanos(json, INACTIVE_THRESHOLD_RE);

        startSeq = JsonUtils.readLong(json, OPT_START_SEQ_RE);
        maxDeliver = JsonUtils.readInteger(json, MAX_DELIVER_RE);
        rateLimit = JsonUtils.readLong(json, RATE_LIMIT_BPS_RE);
        maxAckPending = JsonUtils.readInteger(json, MAX_ACK_PENDING_RE);
        maxPullWaiting = JsonUtils.readInteger(json, MAX_WAITING_RE);
        maxBatch = JsonUtils.readInteger(json, MAX_BATCH_RE);
        maxBytes = JsonUtils.readInteger(json, MAX_BYTES_RE);

        flowControl = JsonUtils.readBoolean(json, FLOW_CONTROL_RE, null);
        headersOnly = JsonUtils.readBoolean(json, HEADERS_ONLY_RE, null);

        backoff = JsonUtils.getDurationList(BACKOFF, json);
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
        this.maxExpires = b.maxExpires;
        this.inactiveThreshold = b.inactiveThreshold;

        this.startSeq = b.startSeq;
        this.maxDeliver = b.maxDeliver;
        this.rateLimit = b.rateLimit;
        this.maxAckPending = b.maxAckPending;
        this.maxPullWaiting = b.maxPullWaiting;
        this.maxBatch = b.maxBatch;
        this.maxBytes = b.maxBytes;

        this.flowControl = b.flowControl;
        this.headersOnly = b.headersOnly;

        this.backoff = b.backoff;
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
        JsonUtils.addFieldWhenGtZero(sb, OPT_START_SEQ, startSeq);
        JsonUtils.addField(sb, OPT_START_TIME, startTime);
        JsonUtils.addField(sb, ACK_POLICY, GetOrDefault(ackPolicy).toString());
        JsonUtils.addFieldAsNanos(sb, ACK_WAIT, ackWait);
        JsonUtils.addFieldWhenGtZero(sb, MAX_DELIVER, maxDeliver);
        JsonUtils.addField(sb, MAX_ACK_PENDING, maxAckPending);
        JsonUtils.addField(sb, FILTER_SUBJECT, filterSubject);
        JsonUtils.addField(sb, REPLAY_POLICY, GetOrDefault(replayPolicy).toString());
        JsonUtils.addField(sb, SAMPLE_FREQ, sampleFrequency);
        JsonUtils.addFieldWhenGtZero(sb, RATE_LIMIT_BPS, rateLimit);
        JsonUtils.addFieldAsNanos(sb, IDLE_HEARTBEAT, idleHeartbeat);
        JsonUtils.addFldWhenTrue(sb, FLOW_CONTROL, flowControl);
        JsonUtils.addField(sb, ApiConstants.MAX_WAITING, maxPullWaiting);
        JsonUtils.addFldWhenTrue(sb, HEADERS_ONLY, headersOnly);
        JsonUtils.addField(sb, MAX_BATCH, maxBatch);
        JsonUtils.addField(sb, MAX_BYTES, maxBytes);
        JsonUtils.addFieldAsNanos(sb, MAX_EXPIRES, maxExpires);
        JsonUtils.addFieldAsNanos(sb, INACTIVE_THRESHOLD, inactiveThreshold);
        JsonUtils.addDurations(sb, BACKOFF, backoff);
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
        return getOrUnsetUlong(startSeq);
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
    public int getMaxDeliver() {
        return getOrUnset(maxDeliver);
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
     * @return the rate limit in bits per second
     */
    public long getRateLimit() {
        return getOrUnsetUlong(rateLimit);
    }

    /**
     * Gets the maximum ack pending configuration.
     * @return maximum ack pending.
     */
    public int getMaxAckPending() {
        return getOrUnset(maxAckPending);
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
        // The way the builder and json reading works it's never false if it's not null
        // but this way I can make code coverage happy and not assume.
        return Boolean.TRUE.equals(flowControl);
    }

    /**
     * Get the number of pulls that can be outstanding on a pull consumer
     * @return the max pull waiting
     */
    public int getMaxPullWaiting() {
        return getOrUnset(maxPullWaiting);
    }

    /**
     * Get the header only flag indicating whether it's on or off
     * @return the flow control mode
     */
    public boolean isHeadersOnly() {
        return headersOnly != null && headersOnly;
    }

    /**
     * Get the max batch size for the server to allow on pull requests.
     * @return the max batch size
     */
    public int getMaxBatch() {
        return getOrUnset(maxBatch);
    }

    /**
     * Get the max bytes size for the server to allow on pull requests.
     * @return the max byte size
     */
    public int getMaxBytes() {
        return getOrUnset(maxBytes);
    }

    /**
     * Get the max amount of expire time for the server to allow on pull requests.
     * @return the max expire
     */
    public Duration getMaxExpires() {
        return maxExpires;
    }

    /**
     * Get the amount of time before the ephemeral consumer is deemed inactive.
     * @return the inactive threshold
     */
    public Duration getInactiveThreshold() {
        return inactiveThreshold;
    }

    /**
     * Get the backoff list; may be empty, will never be null.
     * @return the list
     */
    public List<Duration> getBackoff() {
        return backoff;
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
     * @return true if the start sequence was set by the user
     */
    public boolean startSeqWasSet() {
        return startSeq != null;
    }

    /**
     * Gets whether max deliver for this consumer configuration was set or left unset
     * @return true if max deliver was set by the user
     */
    public boolean maxDeliverWasSet() {
        return maxDeliver != null;
    }

    /**
     * Gets whether rate limit for this consumer configuration was set or left unset
     * @return true if rate limit was set by the user
     */
    public boolean rateLimitWasSet() {
        return rateLimit != null;
    }

    /**
     * Gets whether max ack pending for this consumer configuration was set or left unset
     * @return true if mac ack pending was set by the user
     */
    public boolean maxAckPendingWasSet() {
        return maxAckPending != null;
    }

    /**
     * Gets whether max pull waiting for this consumer configuration was set or left unset
     * @return true if max pull waiting was set by the user
     */
    public boolean maxPullWaitingWasSet() {
        return maxPullWaiting != null;
    }

    /**
     * Gets whether max batch for this consumer configuration was set or left unset
     * @return true if max batch was set by the user
     */
    public boolean maxBatchWasSet() {
        return maxBatch != null;
    }

    /**
     * Gets whether max bytes for this consumer configuration was set or left unset
     * @return true if max bytes was set by the user
     */
    public boolean maxBytesWasSet() {
        return maxBytes != null;
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
        private Duration maxExpires;
        private Duration inactiveThreshold;

        private Long startSeq;
        private Integer maxDeliver;
        private Long rateLimit;
        private Integer maxAckPending;
        private Integer maxPullWaiting;
        private Integer maxBatch;
        private Integer maxBytes;

        private Boolean flowControl;
        private Boolean headersOnly;

        private List<Duration> backoff = new ArrayList<>();

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
                this.maxExpires = cc.maxExpires;
                this.inactiveThreshold = cc.inactiveThreshold;

                this.startSeq = cc.startSeq;
                this.maxDeliver = cc.maxDeliver;
                this.rateLimit = cc.rateLimit;
                this.maxAckPending = cc.maxAckPending;
                this.maxPullWaiting = cc.maxPullWaiting;
                this.maxBatch = cc.maxBatch;
                this.maxBytes = cc.maxBytes;

                this.flowControl = cc.flowControl;
                this.headersOnly = cc.headersOnly;

                this.backoff = new ArrayList<>(cc.backoff);
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
         * @param subject the subject.
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
            this.startSeq = normalizeUlong(sequence);
            return this;
        }

        /**
         * Sets the start sequence of the ConsumerConfiguration.
         * @param sequence the start sequence
         * @return Builder
         */
        public Builder startSequence(long sequence) {
            this.startSeq = normalizeUlong(sequence);
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
            this.ackWait = normalize(timeout);
            return this;
        }

        /**
         * Sets the acknowledgement wait duration of the ConsumerConfiguration.
         * @param timeoutMillis the wait timeout in milliseconds
         * @return Builder
         */
        public Builder ackWait(long timeoutMillis) {
            this.ackWait = normalizeDuration(timeoutMillis);
            return this;
        }

        /**
         * Sets the maximum delivery amount of the ConsumerConfiguration or null to unset / clear.
         * @param maxDeliver the maximum delivery amount
         * @return Builder
         */
        public Builder maxDeliver(Long maxDeliver) {
            this.maxDeliver = normalizeToInt(maxDeliver, MAX_DELIVER_MIN);
            return this;
        }

        /**
         * Sets the maximum delivery amount of the ConsumerConfiguration.
         * @param maxDeliver the maximum delivery amount
         * @return Builder
         */
        public Builder maxDeliver(long maxDeliver) {
            this.maxDeliver = normalizeToInt(maxDeliver, MAX_DELIVER_MIN);
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
         * @param bitsPerSecond bits per second to deliver
         * @return Builder
         */
        public Builder rateLimit(Long bitsPerSecond) {
            this.rateLimit = normalizeUlong(bitsPerSecond);
            return this;
        }

        /**
         * Set the rate limit of the ConsumerConfiguration.
         * @param bitsPerSecond bits per second to deliver
         * @return Builder
         */
        public Builder rateLimit(long bitsPerSecond) {
            this.rateLimit = normalizeUlong(bitsPerSecond);
            return this;
        }

        /**
         * Sets the maximum ack pending or null to unset / clear.
         * @param maxAckPending maximum pending acknowledgements.
         * @return Builder
         */
        public Builder maxAckPending(Long maxAckPending) {
            this.maxAckPending = normalizeToInt(maxAckPending, STANDARD_MIN);
            return this;
        }

        /**
         * Sets the maximum ack pending.
         * @param maxAckPending maximum pending acknowledgements.
         * @return Builder
         */
        public Builder maxAckPending(long maxAckPending) {
            this.maxAckPending = normalizeToInt(maxAckPending, STANDARD_MIN);
            return this;
        }

        /**
         * sets the idle heart beat wait time
         * @param idleHeartbeat the idle heart beat duration
         * @return Builder
         */
        public Builder idleHeartbeat(Duration idleHeartbeat) {
            if (idleHeartbeat == null) {
                this.idleHeartbeat = null;
            }
            else {
                long nanos = idleHeartbeat.toNanos();
                if (nanos <= DURATION_UNSET_LONG) {
                    this.idleHeartbeat = DURATION_UNSET;
                }
                else if (nanos < MIN_IDLE_HEARTBEAT_NANOS) {
                    throw new IllegalArgumentException("Duration must be greater than or equal to " + MIN_IDLE_HEARTBEAT_NANOS + " nanos.");
                }
                else {
                    this.idleHeartbeat = idleHeartbeat;
                }
            }
            return this;
        }

        /**
         * sets the idle heart beat wait time
         * @param idleHeartbeatMillis the idle heart beat duration in milliseconds
         * @return Builder
         */
        public Builder idleHeartbeat(long idleHeartbeatMillis) {
            if (idleHeartbeatMillis <= DURATION_UNSET_LONG) {
                this.idleHeartbeat = DURATION_UNSET;
            }
            else if (idleHeartbeatMillis < MIN_IDLE_HEARTBEAT_MILLIS) {
                throw new IllegalArgumentException("Duration must be greater than or equal to " + MIN_IDLE_HEARTBEAT_MILLIS + " milliseconds.");
            }
            else {
                this.idleHeartbeat = Duration.ofMillis(idleHeartbeatMillis);
            }
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
         * sets the max amount of expire time for the server to allow on pull requests.
         * @param maxExpires the max expire duration
         * @return Builder
         */
        public Builder maxExpires(Duration maxExpires) {
            this.maxExpires = normalize(maxExpires);
            return this;
        }

        /**
         * sets the max amount of expire time for the server to allow on pull requests.
         * @param maxExpires the max expire duration in milliseconds
         * @return Builder
         */
        public Builder maxExpires(long maxExpires) {
            this.maxExpires = normalizeDuration(maxExpires);
            return this;
        }

        /**
         * sets the amount of time before the ephemeral consumer is deemed inactive.
         * @param inactiveThreshold the threshold duration
         * @return Builder
         */
        public Builder inactiveThreshold(Duration inactiveThreshold) {
            this.inactiveThreshold = normalize(inactiveThreshold);
            return this;
        }

        /**
         * sets the amount of time before the ephemeral consumer is deemed inactive.
         * @param inactiveThreshold the threshold duration in milliseconds
         * @return Builder
         */
        public Builder inactiveThreshold(long inactiveThreshold) {
            this.inactiveThreshold = normalizeDuration(inactiveThreshold);
            return this;
        }

        /**
         * sets the max pull waiting, the number of pulls that can be outstanding on a pull consumer, pulls received after this is reached are ignored.
         * Use null to unset / clear.
         * @param maxPullWaiting the max pull waiting
         * @return Builder
         */
        public Builder maxPullWaiting(Long maxPullWaiting) {
            this.maxPullWaiting = normalizeToInt(maxPullWaiting, STANDARD_MIN);
            return this;
        }

        /**
         * sets the max pull waiting, the number of pulls that can be outstanding on a pull consumer, pulls received after this is reached are ignored.
         * @param maxPullWaiting the max pull waiting
         * @return Builder
         */
        public Builder maxPullWaiting(long maxPullWaiting) {
            this.maxPullWaiting = normalizeToInt(maxPullWaiting, STANDARD_MIN);
            return this;
        }

        /**
         * sets the max batch size for the server to allow on pull requests.
         * @param maxBatch the max batch size
         * @return Builder
         */
        public Builder maxBatch(Long maxBatch) {
            this.maxBatch = normalizeToInt(maxBatch, STANDARD_MIN);
            return this;
        }

        /**
         * sets the max batch size for the server to allow on pull requests.
         * @param maxBatch the max batch size
         * @return Builder
         */
        public Builder maxBatch(long maxBatch) {
            this.maxBatch = normalizeToInt(maxBatch, STANDARD_MIN);
            return this;
        }

        /**
         * sets the max bytes size for the server to allow on pull requests.
         * @param maxBytes the max bytes size
         * @return Builder
         */
        public Builder maxBytes(Long maxBytes) {
            this.maxBytes = normalizeToInt(maxBytes, STANDARD_MIN);
            return this;
        }

        /**
         * sets the max bytes size for the server to allow on pull requests.
         * @param maxBytes the max bytes size
         * @return Builder
         */
        public Builder maxBytes(long maxBytes) {
            this.maxBytes = normalizeToInt(maxBytes, STANDARD_MIN);
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
         * Set the list of backoff.
         * @param backoffs zero or more backoff durations or an array of backoffs
         * @return Builder
         */
        public Builder backoff(Duration... backoffs) {
            this.backoff.clear();
            if (backoffs != null) {
                for (Duration d : backoffs) {
                    if (d != null) {
                        if (d.toNanos() < DURATION_MIN_LONG)
                        {
                            throw new IllegalArgumentException("Backoff cannot be less than " + DURATION_MIN_LONG);
                        }
                        this.backoff.add(d);
                    }
                }
            }
            return this;
        }

        /**
         * Set the list of backoff.
         * @param backoffsMillis zero or more backoff in millis or an array of backoffsMillis
         * @return Builder
         */
        public Builder backoff(long... backoffsMillis) {
            this.backoff.clear();
            if (backoffsMillis != null) {
                for (long ms : backoffsMillis) {
                    if (ms < DURATION_MIN_LONG) {
                        throw new IllegalArgumentException("Backoff cannot be less than " + DURATION_MIN_LONG);
                    }
                    this.backoff.add(Duration.ofMillis(ms));
                }
            }
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
            ", maxBatch=" + maxBatch +
            ", maxBytes=" + maxBytes +
            ", maxExpires=" + maxExpires +
            ", inactiveThreshold=" + inactiveThreshold +
            ", backoff=" + backoff +
            '}';
    }

    protected static int getOrUnset(Integer val)
    {
        return val == null ? INTEGER_UNSET : val;
    }

    protected static long getOrUnsetUlong(Long val)
    {
        return val == null || val < 0 ? ULONG_UNSET : val;
    }

    protected static Duration getOrUnset(Duration val)
    {
        return val == null ? DURATION_UNSET : val;
    }

    protected static Integer normalizeToInt(Long l, int min) {
        if (l == null) {
            return null;
        }

        if (l < min) {
            return INTEGER_UNSET;
        }

        if (l > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }

        return l.intValue();
    }

    protected static Long normalizeUlong(Long u)
    {
        return u == null ? null : u <= ULONG_UNSET ? ULONG_UNSET : u;
    }

    protected static Duration normalize(Duration d)
    {
        return d == null ? null : d.toNanos() <= DURATION_UNSET_LONG ? DURATION_UNSET : d;
    }

    protected static Duration normalizeDuration(long millis)
    {
        return millis <= DURATION_UNSET_LONG ? DURATION_UNSET : Duration.ofMillis(millis);
    }

    protected static DeliverPolicy GetOrDefault(DeliverPolicy p) { return p == null ? DEFAULT_DELIVER_POLICY : p; }
    protected static AckPolicy GetOrDefault(AckPolicy p) { return p == null ? DEFAULT_ACK_POLICY : p; }
    protected static ReplayPolicy GetOrDefault(ReplayPolicy p) { return p == null ? DEFAULT_REPLAY_POLICY : p; }
}
