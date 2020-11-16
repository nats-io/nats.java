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

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


// TODO Add properties

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
        static
        {
            for(DeliverPolicy env : DeliverPolicy.values())
            {
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
        static
        {
            for(AckPolicy env : AckPolicy.values())
            {
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
        static
        {
            for(ReplayPolicy env : ReplayPolicy.values())
            {
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
	private long startSeq = -1;
	private ZonedDateTime startTime = null;
	private AckPolicy ackPolicy = AckPolicy.All;
	private Duration ackWait = Duration.ZERO;
	private long maxDeliver = -1;
	private String filterSubject = null;
	private ReplayPolicy replayPolicy = ReplayPolicy.Instant;
	private String sampleFrequency = null;
	private long rateLimit = -1;

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

    private static final String grabString = "\\s*\"(.+?)\"";
    private static final String grabNumber = "\\s*(\\d+)";

    // TODO - replace with a safe (risk-wise) JSON parser
    private static final Pattern durableRE = Pattern.compile("\""+ durableNameField + "\":" + grabString, Pattern.CASE_INSENSITIVE); 
    private static final Pattern deliverSubjectRE = Pattern.compile("\""+ deliverSubjField + "\":" + grabString, Pattern.CASE_INSENSITIVE); 
    private static final Pattern deliverPolicyRE = Pattern.compile("\""+ deliverPolicyField + "\":" + grabString, Pattern.CASE_INSENSITIVE); 
    private static final Pattern startSeqRE = Pattern.compile("\""+ startSeqField + "\":" + grabNumber, Pattern.CASE_INSENSITIVE); 
    private static final Pattern startTimeRE = Pattern.compile("\""+ startTimeField + "\":" + grabString, Pattern.CASE_INSENSITIVE); 
    private static final Pattern ackPolicyRE = Pattern.compile("\""+ ackPolicyField + "\":" + grabString, Pattern.CASE_INSENSITIVE); 
    private static final Pattern ackWaitRE = Pattern.compile("\""+ ackWaitField + "\":" + grabNumber, Pattern.CASE_INSENSITIVE); 
    private static final Pattern maxDeliverRE = Pattern.compile("\""+ maxDeliverField + "\":" + grabNumber, Pattern.CASE_INSENSITIVE); 
    private static final Pattern filterSubjectRE = Pattern.compile("\""+ filterSubjectField + "\":" + grabString, Pattern.CASE_INSENSITIVE); 
    private static final Pattern replayPolicyRE = Pattern.compile("\""+ replayPolicyField + "\":" + grabString, Pattern.CASE_INSENSITIVE); 
    private static final Pattern sampleFreqRE = Pattern.compile("\""+ sampleFreqField + "\":" + grabString, Pattern.CASE_INSENSITIVE); 
    private static final Pattern rateLimitRE = Pattern.compile("\""+ rateLimitField + "\":" + grabNumber, Pattern.CASE_INSENSITIVE); 

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

    }

    // For the builder
    ConsumerConfiguration(String durable, DeliverPolicy deliverPolicy, long startSeq,
            ZonedDateTime startTime, AckPolicy ackPolicy, Duration ackWait, long maxDeliver, String filterSubject,
            ReplayPolicy replayPolicy, String sampleFrequency, long rateLimit) {

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
    }

    private static void addFld(StringBuilder sb, String fname, String value) {
        if (value != null) {
            sb.append("\"" + fname + "\" : \"" + value + "\",");
        }
    }

    private static void addFld(StringBuilder sb, String fname, long value) {
        if (value >= 0) {
            sb.append("\"" + fname + "\" : " + value + ",");
        }      
    }

    private static void addFld(StringBuilder sb, String fname, Duration value) {
        if (value != Duration.ZERO) {
            sb.append("\"" + fname + "\" : " + value.toNanos() + ",");
        }       
    }
    
    private static void addFld(StringBuilder sb, String fname, ZonedDateTime time) {
        if (time == null) {
            return;
        }

        String s = Nats.rfc3339Formatter.format(time);
        sb.append("\"" + fname + "\" : \"" + s + "\",");
    }

    /**
     * Returns a JSON representation of this consumer configuration.
     * 
     * @param streamName name of the stream.
     * @return json consumer configuration to send to the server.
     */
    public String toJSON(String streamName) {
        
        StringBuilder sb = new StringBuilder("{");
        
        addFld(sb, "stream_name", streamName);
        
        sb.append("\"config\" : {");
        
        addFld(sb, durableNameField, durable);
        addFld(sb, deliverSubjField, deliverSubject);
        addFld(sb, deliverPolicyField, deliverPolicy.toString());
        addFld(sb, startSeqField, startSeq);
        addFld(sb, startTimeField, startTime);
        addFld(sb, ackPolicyField, ackPolicy.toString());
        addFld(sb, ackWaitField, ackWait);
        addFld(sb, maxDeliverField, maxDeliver);
        addFld(sb, filterSubjectField, filterSubject);
        addFld(sb, replayPolicyField, replayPolicy.toString());
        addFld(sb, sampleFreqField, sampleFrequency);
        addFld(sb, rateLimitField, rateLimit);

        // remove the trailing ','
        sb.setLength(sb.length()-1);

        sb.append("}}");

        return sb.toString();
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
    public void setDeliverySubject(String subject) {
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
        private AckPolicy ackPolicy = AckPolicy.All;
        private Duration ackWait = Duration.ofSeconds(30);
        private long maxDeliver = -1;
        private String filterSubject = null;
        private ReplayPolicy replayPolicy = ReplayPolicy.Instant;
        private String sampleFrequency = null;
        private long rateLimit = 0;
    
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
        public Builder maxDelivery(long maxDeliver) {
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
                rateLimit
            );
        }

    }
}
