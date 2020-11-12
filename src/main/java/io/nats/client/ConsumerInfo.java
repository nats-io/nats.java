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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// TODO Add properties

/**
 * The ConsumerInfo class returns information about a jetstream consumer.
 */
public class ConsumerInfo {

    /**
     * This class holds the sequence numbers for a consumer and 
     * stream.
     */
    public class SequencePair {
        private long consumerSeq = -1;
        private long streamSeq = -1;

        SequencePair(String json) {
            Matcher m = consumerSeqRE.matcher(json);
            if (m.find()) {
                this.consumerSeq = Long.parseLong(m.group(1));
            }
           
            m = streamSeqRE.matcher(json);
            if (m.find()) {
                this.streamSeq = Long.parseLong(m.group(1));
            }            
        }

        /**
         * Gets the consumer sequence number.
         * @return seqence number.
         */
        public long getConsumerSequence() {
            return consumerSeq;
        }

        /**
         * Gets the stream sequence number.
         * @return sequence number.
         */
        public long getStreamSequence() {
            return streamSeq;
        }
    }

    private String stream;
    private String name;
    private ConsumerConfiguration configuration;
    private LocalDateTime created;
    private SequencePair delivered;
    private SequencePair ackFloor;
    private long numPending;
    private long numRelivered;
    
    private static final String streamNameField =  "stream_name";
    private static final String nameField = "name";
    private static final String configField =  "config";
    private static final String createdField =  "created";
    private static final String deliveredField =  "delivered";
    private static final String ackFloorField =  "ack_floor";
    private static final String numPendingField =  "num_pending";
    private static final String numRedelivered =  "num_redelivered";
    private static final String streamSeqField = "stream_seq";
    private static final String consumerSeqField = "consumer_seq";
   
    private static final String grabString = "\\s*\"(.+?)\"";
    private static final String grabNumber = "\\s*(\\d+)";

    // TODO - replace with a safe (risk-wise) JSON parser
    private static final Pattern streamNameRE = Pattern.compile("\""+ streamNameField + "\":" + grabString, Pattern.CASE_INSENSITIVE); 
    private static final Pattern nameRE = Pattern.compile("\""+ nameField + "\":" + grabString, Pattern.CASE_INSENSITIVE); 
    private static final Pattern createdRE = Pattern.compile("\""+ createdField + "\":" + grabString, Pattern.CASE_INSENSITIVE); 
    private static final Pattern numPendingRE = Pattern.compile("\""+ numPendingField + "\":" + grabNumber, Pattern.CASE_INSENSITIVE); 
    private static final Pattern numRedeliveredRE = Pattern.compile("\""+ numRedelivered + "\":" + grabNumber, Pattern.CASE_INSENSITIVE); 
    private static final Pattern consumerSeqRE = Pattern.compile("\""+ consumerSeqField + "\":" + grabNumber, Pattern.CASE_INSENSITIVE); 
    private static final Pattern streamSeqRE = Pattern.compile("\""+ streamSeqField + "\":" + grabNumber, Pattern.CASE_INSENSITIVE); 
 

    // extract a simple inner json object
    private String getJSONObject(String objectName, String json) {

        int objStart = json.indexOf(objectName);
        if (objStart < 0) {
            return null;
        }

        int bracketStart = json.indexOf("{", objStart);
        int bracketEnd = json.indexOf("}", bracketStart);

        if (bracketStart < 0 || bracketEnd < 0) {
            return null;
        }

        return json.substring(bracketStart, bracketEnd+1);
    }

    /**
     * Internal method to generate consumer information.
     * @param json JSON represeenting the consumer information.
     */
    public ConsumerInfo(String json) {
        Matcher m = streamNameRE.matcher(json);
        if (m.find()) {
            this.stream = m.group(1);
        }
        
        m = nameRE.matcher(json);
        if (m.find()) {
            // todo - double check
            this.name = m.group(1);
        }

        m = createdRE.matcher(json);
        if (m.find()) {
            // Instant can parse rfc 3339... we're making a time zone assumption.
            // TODO - figure this out - will the NATS server always be zulu?
            Instant inst = Instant.parse(m.group(1));
            this.created = LocalDateTime.ofInstant(inst, ZoneId.systemDefault());
        }

        String s = getJSONObject(configField, json);
        if (s != null) {
            this.configuration = new ConsumerConfiguration(s);
        }
  
        s = getJSONObject(deliveredField, json);
        if (s != null) {
            this.delivered = new SequencePair(s);
        }
          
        s = getJSONObject(ackFloorField, json);
        if (s != null) {
            this.ackFloor = new SequencePair(s);
        }

        m = numPendingRE.matcher(json);
        if (m.find()) {
            this.numPending = Long.parseLong(m.group(1));
        }

        m = numRedeliveredRE.matcher(json);
        if (m.find()) {
            // todo - double check
            this.numRelivered = Long.parseLong(m.group(1));
        }
    }
    
    public ConsumerConfiguration getConsumerConfiguration() {
        return configuration;
    }

    public String getName() {
        return name;
    }

    public String getStreamName() {
        return stream;
    }

    public LocalDateTime getCreationTime() {
        return created;
    }

    public SequencePair getDelivered() {
        return delivered;
    }

    public SequencePair getAckFloor() {
        return ackFloor;
    }

    public long getNumPending() {
        return numPending;
    }

    public long getRedelivered() {
        return numRelivered;
    }
}
