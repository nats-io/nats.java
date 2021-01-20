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

import io.nats.client.impl.JsonUtils;
import io.nats.client.impl.JsonUtils.FieldType;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The ConsumerConfiguration class specifies the configuration for creating a jetstream consumer on the client and 
 * if necessary the server.
 * Options are created using a {@link PublishOptions.Builder Builder}.
 */
public class ConsumerConfiguration {

    /**
     * The delivery policy for this consumer.
     */
    public enum DeliverPolicy {
        All("all"),
        Last("last"),
        New("new"),
        ByStartSequence("by_start_sequence"),
        ByStartTime("by_start_time");

        private String policy;

        DeliverPolicy(String p) {
            policy = p;
        }

        @Override
        public String toString() {
            return policy;
        }
        
        private static final Map<String, DeliverPolicy> strEnumHash = new HashMap<>();
        static {
            for(DeliverPolicy env : DeliverPolicy.values()) {
                strEnumHash.put(env.toString(), env);
            }
        }

        public static DeliverPolicy get(String value) {
            return strEnumHash.get(value);
        }
    }

    /**
      * Represents the Ack Policy of a consumer
     */  
    public enum AckPolicy {
        None("none"),
        All("all"),
        Explicit("explicit");

        private String policy;

        AckPolicy(String p) {
            policy = p;
        }

        @Override
        public String toString() {
            return policy;
        }

        private static final Map<String, AckPolicy> strEnumHash = new HashMap<>();
        static {
            for(AckPolicy env : AckPolicy.values()) {
                strEnumHash.put(env.toString(), env);
            }
        }

        public static AckPolicy get(String value) {
            return strEnumHash.get(value);
        }
    }

    /**
     * Represents the replay policy of a consumer.
     */
    public enum ReplayPolicy {
        Instant("instant"),
        Original("original");

        private String policy;

        ReplayPolicy(String p) {
            this.policy = p;
        }

        @Override
        public String toString() {
            return policy;
        }

        private static final Map<String, ReplayPolicy> strEnumHash = new HashMap<>();
        static {
            for(ReplayPolicy env : ReplayPolicy.values()) {
                strEnumHash.put(env.toString(), env);
            }
        }

        public static ReplayPolicy get(String value) {
            return strEnumHash.get(value);
        }        
    }

    private String durable = null;
    private DeliverPolicy deliverPolicy = DeliverPolicy.All;
    private String deliverSubject = null;
    private long startSeq = 0;
    private ZonedDateTime startTime = null;
    private AckPolicy ackPolicy = AckPolicy.Explicit;
    private Duration ackWait = Duration.ofSeconds(30);
    private long maxDeliver = 0;
    private String filterSubject = null;
    private ReplayPolicy replayPolicy = ReplayPolicy.Instant;
    private String sampleFrequency = null;
    private long rateLimit = 0;
    private long maxWaiting = 0;
    private long maxAckPending = 0;

    private static String durableNameField =  "durable_name";
    private static String deliverSubjField = "deliver_subject";
    private static String deliverPolicyField =  "deliver_policy";
    private static String startSeqField =  "opt_start_seq";
    private static String startTimeField=  "opt_start_time";
    private static String ackPolicyField =  "ack_policy";
    private static String ackWaitField =  "ack_wait";
    private static String maxDeliverField =  "max_deliver";
    private static String filterSubjectField =  "filter_subject";
    private static String replayPolicyField =  "replay_policy";
    private static String sampleFreqField =  "sample_frequency";
    private static String rateLimitField =  "rate_limit";    
    private static String maxWaitingField =  "max_waiting";
    private static String maxAckPendingField =  "max_ack_pending";

    private static final Pattern durableRE = JsonUtils.buildPattern(durableNameField, FieldType.jsonString);
    private static final Pattern deliverSubjectRE = JsonUtils.buildPattern(deliverSubjField, FieldType.jsonString);
    private static final Pattern deliverPolicyRE = JsonUtils.buildPattern(deliverPolicyField, FieldType.jsonString);
    private static final Pattern startSeqRE = JsonUtils.buildPattern(startSeqField, FieldType.jsonNumber);
    private static final Pattern startTimeRE = JsonUtils.buildPattern(startTimeField, FieldType.jsonString);
    private static final Pattern ackPolicyRE = JsonUtils.buildPattern(ackPolicyField, FieldType.jsonString);
    private static final Pattern ackWaitRE = JsonUtils.buildPattern(ackWaitField, FieldType.jsonNumber);
    private static final Pattern maxDeliverRE = JsonUtils.buildPattern(maxDeliverField, FieldType.jsonNumber);
    private static final Pattern filterSubjectRE = JsonUtils.buildPattern(filterSubjectField, FieldType.jsonString);
    private static final Pattern replayPolicyRE = JsonUtils.buildPattern(replayPolicyField, FieldType.jsonString);
    private static final Pattern sampleFreqRE = JsonUtils.buildPattern(sampleFreqField, FieldType.jsonString);
    private static final Pattern rateLimitRE = JsonUtils.buildPattern(rateLimitField, FieldType.jsonNumber);
    private static final Pattern maxWaitingRE = JsonUtils.buildPattern(maxWaitingField, FieldType.jsonNumber);
    private static final Pattern maxAckPendingRE = JsonUtils.buildPattern(maxAckPendingField, FieldType.jsonNumber);

    // for the response from the server
    ConsumerConfiguration(String json) {
        Matcher m = durableRE.matcher(json);
        if (m.find()) {
            this.durable = m.group(1);
        }
        
        m = deliverPolicyRE.matcher(json);
        if (m.find()) {
            // todo - double check
            this.deliverPolicy = DeliverPolicy.get(m.group(1));
        }

        m = deliverSubjectRE.matcher(json);
        if (m.find()) {
            this.deliverSubject = m.group(1);
        }        

        m = startSeqRE.matcher(json);
        if (m.find()) {
            this.startSeq = Long.parseLong(m.group(1));
        }

        m = startTimeRE.matcher(json);
        if (m.find()) {
            // Instant can parse rfc 3339... we're making a time zone assumption.
            Instant inst = Instant.parse(m.group(1));
            this.startTime = ZonedDateTime.ofInstant(inst, ZoneId.systemDefault());
        }

        m = ackPolicyRE.matcher(json);
        if (m.find()) {
            // todo - double check
            this.ackPolicy = AckPolicy.get(m.group(1));
        }

        m = ackWaitRE.matcher(json);
        if (m.find()) {
            this.ackWait = Duration.ofNanos(Long.parseLong(m.group(1)));
        } 

        m = maxDeliverRE.matcher(json);
        if (m.find()) {
            this.maxDeliver = Long.parseLong(m.group(1));
        }
        
        m = filterSubjectRE.matcher(json);
        if (m.find()) {
            this.filterSubject = m.group(1);
        } 

        m = replayPolicyRE.matcher(json);
        if (m.find()) {
            // todo - double check
            this.replayPolicy = ReplayPolicy.get(m.group(1));
        }        

        m = sampleFreqRE.matcher(json);
        if (m.find()) {
            // todo - double check
            this.sampleFrequency = m.group(1);
        } 
        
        m = rateLimitRE.matcher(json);
        if (m.find()) {
            this.rateLimit = Long.parseLong(m.group(1));
        }
        
        m = maxAckPendingRE.matcher(json);
        if (m.find()) {
            this.maxAckPending = Long.parseLong(m.group(1));
        } 
        m = maxWaitingRE.matcher(json);
        if (m.find()) {
            this.maxWaiting = Long.parseLong(m.group(1));
        }                 

    }

    // copy constructor for subscriptions
    ConsumerConfiguration(ConsumerConfiguration cc) {
        this(cc.durable, cc.deliverPolicy, cc.startSeq,
            cc.startTime, cc.ackPolicy, cc.ackWait, cc.maxDeliver, cc.filterSubject,
            cc.replayPolicy, cc.sampleFrequency, cc.rateLimit, cc.deliverSubject,
            cc.maxWaiting, cc.maxAckPending);
    }

    // For the builder
    ConsumerConfiguration(String durable, DeliverPolicy deliverPolicy, long startSeq,
            ZonedDateTime startTime, AckPolicy ackPolicy, Duration ackWait, long maxDeliver, String filterSubject,
            ReplayPolicy replayPolicy, String sampleFrequency, long rateLimit, String deliverSubject,
            long maxWaiting, long maxAckPending) {

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
                this.maxWaiting = maxWaiting;
    }

    /**
     * Returns a JSON representation of this consumer configuration.
     * 
     * @param streamName name of the stream.
     * @return json consumer configuration to send to the server.
     */
    public String toJSON(String streamName) {
        
        StringBuilder sb = new StringBuilder("{");
        
        JsonUtils.addFld(sb, "stream_name", streamName);
        
        sb.append("\"config\" : {");
        
        JsonUtils.addFld(sb, durableNameField, durable);
        JsonUtils.addFld(sb, deliverSubjField, deliverSubject);
        JsonUtils.addFld(sb, deliverPolicyField, deliverPolicy.toString());
        JsonUtils.addFld(sb, startSeqField, startSeq);
        JsonUtils.addFld(sb, startTimeField, startTime);
        JsonUtils.addFld(sb, ackPolicyField, ackPolicy.toString());
        JsonUtils.addFld(sb, ackWaitField, ackWait);
        JsonUtils.addFld(sb, maxDeliverField, maxDeliver);
        JsonUtils.addFld(sb, maxWaitingField, maxWaiting);
        JsonUtils.addFld(sb, maxAckPendingField, maxAckPending);
        JsonUtils.addFld(sb, filterSubjectField, filterSubject);
        JsonUtils.addFld(sb, replayPolicyField, replayPolicy.toString());
        JsonUtils.addFld(sb, sampleFreqField, sampleFrequency);
        JsonUtils.addFld(sb, rateLimitField, rateLimit);

        // remove the trailing ','
        sb.setLength(sb.length()-1);
        sb.append("}}");

        return sb.toString();
    }

    
    /**
     * Sets the durable name of the configuration.
     * @param value name of the durable
     */
	public void setDurable(String value) {
        durable = value;
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
     * Package level API to set the deliver subject in the creation API.
     * @param subject - Subject to deliver messages.
     */
    public void setDeliverSubject(String subject) {
        this.deliverSubject = subject;
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
     * @return the acknoledgment policy.
     */    
    public AckPolicy getAckPolicy() {
        return ackPolicy;
    }

    /**
     * Gets the acknowledgment wait of this consumer configuration.
     * @return the acknoledgment wait duration.
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
     * Creates a builder for the publish options.
     * @return a publish options builder
     */
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * ConsumerConfiguration is created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     * 
     * <p>{@code new ConsumerConfiguration.Builder().build()} will create a new ConsumerConfiguration.
     * 
     */
    public static class Builder {

        private String durable = null;
        private DeliverPolicy deliverPolicy = DeliverPolicy.All;
        private long startSeq = 0;
        private ZonedDateTime startTime = null;
        private AckPolicy ackPolicy = AckPolicy.Explicit;
        private Duration ackWait = Duration.ofSeconds(30);
        private long maxDeliver = 0;
        private String filterSubject = null;
        private ReplayPolicy replayPolicy = ReplayPolicy.Instant;
        private String sampleFrequency = null;
        private long rateLimit = 0;
        private String deliverSubject = null;
        private long maxWaiting = 0;
        private long maxAckPending = 0;
    
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
            this.deliverPolicy = policy;
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
            this.ackWait = timeout;
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
            this.replayPolicy = policy;
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
         * Sets the maximum waiting amount.
         * @param maxWaiting maximum waiting acknowledgements.
         * @return Builder
         */
        public Builder maxWaiting(long maxWaiting) {
            this.maxWaiting = maxWaiting;
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
                maxWaiting,
                maxAckPending
            );
        }
    }

    /**
     * Sets the filter subject of the configuration.
     * @param subject filter subject.
     */
	public void setFilterSubject(String subject) {
        this.filterSubject = subject;
	}

    /**
     * Gets the maximum ack pending configuration.
     * @return maxumum ack pending.
     */
	public long getMaxAckPending() {
	    return maxAckPending;
	}

    /**
     * Sets the maximum ack pending.
     * @param maxAckPending maximum pending acknowledgements.
     */
	public void setMaxAckPending(long maxAckPending) {
        this.maxAckPending = maxAckPending;
    }
    
    /**
     * Gets the maximum waiting acknowedgements.
     *
     * @return the max waiting value
     */
	public long getMaxWaiting() {
        return maxWaiting;
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
                ", maxWaiting=" + maxWaiting +
                ", maxAckPending=" + maxAckPending +
                '}';
    }
}
