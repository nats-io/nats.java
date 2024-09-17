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

import java.time.ZonedDateTime;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.*;

/**
 * Object used to make a request for special message get requests.
 */
public class MessageGetRequest implements JsonSerializable {
    private final long sequence;
    private final String lastBySubject;
    private final String nextBySubject;
    private final ZonedDateTime startTime;

    public static MessageGetRequest forSequence(long sequence) {
        return builder().sequence(sequence).build();
    }

    public static MessageGetRequest lastForSubject(String subject) {
        return builder().lastBySubject(subject).build();
    }

    public static MessageGetRequest firstForSubject(String subject) {
        return builder().nextBySubject(subject).build();
    }

    public static MessageGetRequest firstForStartTime(ZonedDateTime startTime) {
        return builder().startTime(startTime).build();
    }

    public static MessageGetRequest nextForSubject(long sequence, String subject) {
        return builder().sequence(sequence).nextBySubject(subject).build();
    }

    /**
     * @deprecated use static method forSequence with .serialize instead
     * @param sequence start sequence
     * @return rendered output
     */
    @Deprecated
    public static byte[] seqBytes(long sequence) {
        return forSequence(sequence).serialize();
    }

    /**
     * @deprecated use static method lastForSubject with .serialize instead
     * @param subject filter subject
     * @return rendered output
     */
    @Deprecated
    public static byte[] lastBySubjectBytes(String subject) {
        return lastForSubject(subject).serialize();
    }

    /**
     * @deprecated use static method forSequence instead
     * 
     * @param sequence start sequence number
     */
    @Deprecated
    public MessageGetRequest(long sequence) {
        this(builder().sequence(sequence));
    }

    /**
     * @deprecated use static method lastForSubject instead
     * 
     * @param lastBySubject filter subject
     */
    @Deprecated
    public MessageGetRequest(String lastBySubject) {
        this(builder().lastBySubject(lastBySubject));
    }

    private MessageGetRequest(Builder b) {
        this.sequence = b.sequence;
        this.lastBySubject = b.lastBySubject;
        this.nextBySubject = b.nextBySubject;
        this.startTime = b.startTime;
    }

    public long getSequence() {
        return sequence;
    }

    public String getLastBySubject() {
        return lastBySubject;
    }

    public String getNextBySubject() {
        return nextBySubject;
    }

    public boolean isSequenceOnly() {
        return sequence > 0 && nextBySubject == null;
    }

    public boolean isLastBySubject() {
        return lastBySubject != null;
    }

    public boolean isNextBySubject() {
        return nextBySubject != null;
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        addField(sb, SEQ, sequence);
        addField(sb, LAST_BY_SUBJECT, lastBySubject);
        addField(sb, NEXT_BY_SUBJECT, nextBySubject);
        addField(sb, START_TIME, startTime);
        return endJson(sb).toString();
    }

    /**
     * Creates a builder for the options.
     * @return a {@link MessageGetRequest} builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a builder for the options.
     * @param req the {@link MessageGetRequest}
     * @return a {@link MessageGetRequest} builder
     */
    public static Builder builder(MessageGetRequest req) {
        return req == null ? new Builder() : new Builder(req);
    }

    public static class Builder {
        private long sequence = -1;
        private String lastBySubject = null;
        private String nextBySubject = null;
        private ZonedDateTime startTime = null;

        /**
         * Construct the builder
         */
        public Builder() {}

        /**
         * Construct the builder and initialize values with the existing {@link MessageGetRequest}
         * @param req the {@link MessageGetRequest} to clone
         */
        public Builder(MessageGetRequest req) {
            if (req == null) {
                return;
            }

            this.sequence = req.sequence;
            this.lastBySubject = req.lastBySubject;
            this.nextBySubject = req.nextBySubject;
            this.startTime = req.startTime;
        }

        /**
         * Get a message with the exact sequence.
         * Or when combined with other settings, where message sequence >= provided sequence.
         *
         * @param sequence which matches one message in a stream, or used as a minimum sequence for filtering
         * @return Builder
         */
        public Builder sequence(long sequence) {
            this.sequence = sequence;
            return this;
        }

        /**
         * Get the last message by specified subject.
         *
         * @param lastBySubject the subject to get the last message for
         * @return Builder
         */
        public Builder lastBySubject(String lastBySubject) {
            this.lastBySubject = lastBySubject;
            return this;
        }

        /**
         * Get the next message by specified subject.
         * Either the first message when not combined with sequence,
         * or a next message where message sequence >= provided sequence.
         *
         * @param nextBySubject the subject to get the first/next message for
         * @return Builder
         */
        public Builder nextBySubject(String nextBySubject) {
            this.nextBySubject = nextBySubject;
            return this;
        }

        /**
         * Get message at or after the specified start time.
         *
         * @param startTime the minimum start time of the returned message
         * @return Builder
         */
        public Builder startTime(ZonedDateTime startTime) {
            this.startTime = startTime;
            return this;
        }

        /**
         * Builds the {@link MessageGetRequest} with this configuration.
         * @return MessageGetRequest
         */
        public MessageGetRequest build() {
            return new MessageGetRequest(this);
        }
    }
}
