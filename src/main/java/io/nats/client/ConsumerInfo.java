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

import io.nats.client.support.JsonUtils;
import io.nats.client.support.JsonUtils.FieldType;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
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
    static class SequencePair {
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
    private ZonedDateTime created;
    private SequencePair delivered;
    private SequencePair ackFloor;
    private long numPending;
    private long numWaiting;
    private long numAckPending;
    private long numRelivered;
    
    private static final String streamNameField =  "stream_name";
    private static final String nameField = "name";
    private static final String createdField =  "created";
    private static final String configField =  "config";
    private static final String deliveredField =  "delivered";
    private static final String ackFloorField =  "ack_floor";
    private static final String numAckPendingField =  "num_ack_pending";
    private static final String numRedeliveredField =  "num_redelivered";
    private static final String numWaitingField =  "num_waiting";
    private static final String numPendingField =  "num_pending";

    private static final String streamSeqField = "stream_seq";
    private static final String consumerSeqField = "consumer_seq";
   
    private static final Pattern streamNameRE = JsonUtils.buildPattern(streamNameField, FieldType.jsonString);
    private static final Pattern nameRE = JsonUtils.buildPattern(nameField, FieldType.jsonString);
    private static final Pattern createdRE = JsonUtils.buildPattern(createdField, FieldType.jsonString);
    private static final Pattern numPendingRE = JsonUtils.buildPattern(numPendingField, FieldType.jsonNumber);
    private static final Pattern numAckPendingRE = JsonUtils.buildPattern(numAckPendingField, FieldType.jsonNumber);
    private static final Pattern numRedeliveredRE = JsonUtils.buildPattern(numRedeliveredField, FieldType.jsonNumber); 
    private static final Pattern numWaitingRE = JsonUtils.buildPattern(numWaitingField, FieldType.jsonNumber); 
    private static final Pattern streamSeqRE = JsonUtils.buildPattern(streamSeqField, FieldType.jsonNumber);
    private static final Pattern consumerSeqRE = JsonUtils.buildPattern(consumerSeqField, FieldType.jsonNumber); 

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
            Instant inst = Instant.parse(m.group(1));
            this.created = ZonedDateTime.ofInstant(inst, ZoneId.systemDefault());
        }

        String s = JsonUtils.getJSONObject(configField, json);
        if (s != null) {
            this.configuration = new ConsumerConfiguration(s);
        }
  
        s = JsonUtils.getJSONObject(deliveredField, json);
        if (s != null) {
            this.delivered = new SequencePair(s);
        }
          
        s = JsonUtils.getJSONObject(ackFloorField, json);
        if (s != null) {
            this.ackFloor = new SequencePair(s);
        }

        m = numPendingRE.matcher(json);
        if (m.find()) {
            this.numPending = Long.parseLong(m.group(1));
        }

        m = numWaitingRE.matcher(json);
        if (m.find()) {
            this.numWaiting = Long.parseLong(m.group(1));
        }        

        m = numAckPendingRE.matcher(json);
        if (m.find()) {
            this.numAckPending = Long.parseLong(m.group(1));
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

    public ZonedDateTime getCreationTime() {
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

    public long getNumWaiting() {
        return numWaiting;
    }

    public long getNumAckPending() {
        return numAckPending;
    }

    public long getRedelivered() {
        return numRelivered;
    }
}
