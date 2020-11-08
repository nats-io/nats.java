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

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
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
	private LocalDateTime startTime = null;
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
            // TODO - figure this out - will the NATS server always be zulu?
            Instant inst = Instant.parse(m.group(1));
            this.startTime = LocalDateTime.ofInstant(inst, ZoneId.systemDefault());
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
            LocalDateTime startTime, AckPolicy ackPolicy, Duration ackWait, long maxDeliver, String filterSubject,
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
            sb.append("\"" + fname + "\" : \"" + value + "\"");
        }
    }

    private static void addFld(StringBuilder sb, String fname, long value, boolean comma) {
        if (value >= 0) {
            sb.append("\"" + fname + "\" : " + value + ",");
            if (comma) {
                sb.append(",");
            }  
        }      
    }

    private static void addFld(StringBuilder sb, String fname, Duration value) {
        if (value != Duration.ZERO) {
            sb.append("\"" + fname + "\" : \"" + value.toNanos() + "\",");
        }       
    }    

    String toJSON(String subject) {
        
        StringBuilder sb = new StringBuilder("{");
        
        addFld(sb, "deliver_subject", subject);
        
        sb.append("\"config\" : {");
        
        addFld(sb, durableNameField, durable);
        addFld(sb, deliverSubjField, deliverSubject);
        addFld(sb, deliverPolicyField, deliverPolicy.toString());
        addFld(sb, startSeqField, startSeq, false);
        addFld(sb, startTimeField, startTime.toString());  // TODO - convert time - will ISO-8601 work?
        addFld(sb, ackPolicyField, ackPolicy.toString());
        addFld(sb, ackWaitField, ackWait);
        addFld(sb, maxDeliverField, maxDeliver, false);
        addFld(sb, filterSubjectField, filterSubject);
        addFld(sb, replayPolicyField, replayPolicy.toString());
        addFld(sb, sampleFreqField, sampleFrequency);
        addFld(sb, rateLimitField, rateLimit, true);

        sb.append("}}");

        return sb.toString();
    }

    public String getDurable() {
        return durable;
    }

    public String getDeliverSubject() {
        return deliverSubject;
    }

    /**
     * Package level API to set the deliver subject in the creation API.
     * @param subject - Subject to deliver messages.
     */
    void setDeliverySubject(String subject) {
        this.deliverSubject = subject;
    }

    public DeliverPolicy getDeliverPolicy() {
        return deliverPolicy;
    }

    public long getStartSequence() {
        return startSeq;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public AckPolicy getAckPolicy() {
        return ackPolicy;
    }

    public Duration getAckWait() {
        return ackWait;
    }

    public long getMaxDeliver() {
        return maxDeliver;
    }

    public String getFilterSubject() {
        return filterSubject;
    }

    public ReplayPolicy getReplayPolicy() {
        return replayPolicy;
    }

    public long getRateLimit() {
        return rateLimit;
    }

    /**
     * Creates a builder for the publish options.
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String durable = null;
        private DeliverPolicy deliverPolicy = DeliverPolicy.All;
        private long startSeq = -1;
        private LocalDateTime startTime = null;
        private AckPolicy ackPolicy = AckPolicy.All;
        private Duration ackWait = Duration.ofSeconds(2);
        private long maxDeliver = -1;
        private String filterSubject = null;
        private ReplayPolicy replayPolicy = ReplayPolicy.Instant;
        private String sampleFrequency = null;
        private long rateLimit = -1;
    
        public Builder durable(String durable) {
            this.durable = durable;
            return this;
        }      

        public Builder deliverPolicy(DeliverPolicy policy) {
            this.deliverPolicy = policy;
            return this;
        }

        public Builder startSequence(long sequence) {
            this.startSeq = sequence;
            return this;
        }

        public Builder startTime(LocalDateTime startTime) {
            this.startTime = startTime;
            return this;
        }

        public Builder ackPolicy(AckPolicy policy) {
            this.ackPolicy = policy;
            return this;
        }

        public Builder ackWait(Duration timeout) {
            this.ackWait = timeout;
            return this;
        }

        public Builder maxDelivery(long maxDeliver) {
            this.maxDeliver = maxDeliver;
            return this;
        }

        public Builder filterSubject(String filterSubject) {
            this.filterSubject = filterSubject;
            return this;
        }

        public Builder replayPolicy(ReplayPolicy policy) {
            this.replayPolicy = policy;
            return this;
        }

        public Builder sampleFrequency(String frequency) {
            this.sampleFrequency = frequency;
            return this;
        }

        public Builder rateLimit(int msgsPerSecond) {
            this.rateLimit = msgsPerSecond;
            return this;
        }

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
