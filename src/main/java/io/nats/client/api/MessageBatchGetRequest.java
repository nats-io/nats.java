// Copyright 2024 The NATS Authors
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

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.*;
import static io.nats.client.support.Validator.*;

/**
 * Object used to make a request for message batch get requests.
 */
public class MessageBatchGetRequest implements JsonSerializable {

    private final int batch;
    private final int maxBytes;
    private final long sequence;
    private final ZonedDateTime startTime;
    private final String nextBySubject;
    private final List<String> multiLastFor;
    private final long upToSequence;
    private final ZonedDateTime upToTime;

    MessageBatchGetRequest(Builder b) {
        this.batch = b.batch;
        this.maxBytes = b.maxBytes;
        this.sequence = b.sequence;
        this.startTime = b.startTime;
        this.nextBySubject = b.nextBySubject;
        this.multiLastFor = b.multiLastFor;
        this.upToSequence = b.upToSequence;
        this.upToTime = b.upToTime;
    }

    /**
     * Maximum amount of messages to be returned for this request.
     *
     * @return batch size
     */
    public int getBatch() {
        return batch;
    }

    /**
     * Maximum amount of returned bytes for this request.
     * Limits the amount of returned messages to not exceed this.
     *
     * @return maximum bytes
     */
    public int getMaxBytes() {
        return maxBytes;
    }

    /**
     * Minimum sequence for returned messages.
     * All returned messages will have a sequence equal to or higher than this.
     *
     * @return minimum message sequence
     */
    public long getSequence() {
        return sequence;
    }

    /**
     * Minimum start time for returned messages.
     * All returned messages will have a start time equal to or higher than this.
     *
     * @return minimum message start time
     */
    public ZonedDateTime getStartTime() {
        return startTime;
    }

    /**
     * Subject used to filter messages that should be returned.
     *
     * @return the subject to filter
     */
    public String getSubject() {
        return nextBySubject;
    }

    /**
     * Subjects filter used, these can include wildcards.
     * Will get the last messages matching the subjects.
     *
     * @return the subjects to get the last messages for
     */
    public List<String> getMultiLastForSubjects() {
        return multiLastFor;
    }

    /**
     * Only return messages up to this sequence.
     *
     * @return the maximum message sequence to return results for
     */
    public long getUpToSequence() {
        return upToSequence;
    }

    /**
     * Only return messages up to this time.
     *
     * @return the maximum message time to return results for
     */
    public ZonedDateTime getUpToTime() {
        return upToTime;
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        addField(sb, BATCH, batch);
        addField(sb, MAX_BYTES, maxBytes);
        addField(sb, SEQ, sequence);
        addField(sb, START_TIME, startTime);
        addField(sb, NEXT_BY_SUBJECT, nextBySubject);
        addStrings(sb, MULTI_LAST, multiLastFor);
        addField(sb, UP_TO_SEQ, upToSequence);
        addField(sb, UP_TO_TIME, upToTime);
        return endJson(sb).toString();
    }

    /**
     * Creates a builder for the request.
     *
     * @return Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a builder for the request.
     *
     * @param req the {@link MessageBatchGetRequest}
     * @return Builder
     */
    public static Builder builder(MessageBatchGetRequest req) {
        return req == null ? new Builder() : new Builder(req);
    }

    /**
     * {@link MessageBatchGetRequest} is created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     *
     * <p>{@code MessageBatchGetRequest.builder().build()} will create a default {@link MessageBatchGetRequest}.
     */
    public static class Builder {
        private int batch = -1;
        private int maxBytes = -1;
        private long sequence = -1;
        private ZonedDateTime startTime = null;
        private String nextBySubject = null;
        private List<String> multiLastFor = new ArrayList<>();
        private long upToSequence = -1;
        private ZonedDateTime upToTime = null;

        /**
         * Construct the builder
         */
        public Builder() {
        }

        /**
         * Construct the builder and initialize values with the existing {@link MessageBatchGetRequest}
         *
         * @param req the {@link MessageBatchGetRequest} to clone
         */
        public Builder(MessageBatchGetRequest req) {
            if (req != null) {
                this.batch = req.batch;
                this.maxBytes = req.maxBytes;
                this.sequence = req.sequence;
                this.startTime = req.startTime;
                this.nextBySubject = req.nextBySubject;
                this.multiLastFor = req.multiLastFor;
                this.upToSequence = req.upToSequence;
                this.upToTime = req.upToTime;
            }
        }

        /**
         * Set the maximum amount of messages to be returned for this request.
         *
         * @param batch the batch size
         * @return Builder
         */
        public Builder batch(int batch) {
            validateGtZero(batch, "Request batch size");
            this.batch = batch;
            return this;
        }

        /**
         * Maximum amount of returned bytes for this request.
         * Limits the amount of returned messages to not exceed this.
         *
         * @param maxBytes the maximum bytes
         * @return Builder
         */
        public Builder maxBytes(int maxBytes) {
            this.maxBytes = maxBytes;
            return this;
        }

        /**
         * Minimum sequence for returned messages.
         * All returned messages will have a sequence equal to or higher than this.
         *
         * @param sequence the minimum message sequence
         * @return Builder
         */
        public Builder sequence(long sequence) {
            validateGtEqZero(sequence, "Sequence");
            this.sequence = sequence;
            return this;
        }

        /**
         * Minimum start time for returned messages.
         * All returned messages will have a start time equal to or higher than this.
         *
         * @param startTime the minimum message start time
         * @return Builder
         */
        public Builder startTime(ZonedDateTime startTime) {
            this.startTime = startTime;
            return this;
        }

        /**
         * Subject used to filter messages that should be returned.
         *
         * @param subject the subject to filter
         * @return Builder
         */
        public Builder subject(String subject) {
            this.nextBySubject = subject;
            return this;
        }

        /**
         * Subjects filter used, these can include wildcards.
         * Will get the last messages matching the subjects.
         *
         * @param subjects the subjects to get the last messages for
         * @return Builder
         */
        public Builder multiLastForSubjects(String... subjects) {
            this.multiLastFor.clear();
            this.multiLastFor.addAll(Arrays.asList(subjects));
            return this;
        }

        /**
         * Subjects filter used, these can include wildcards.
         * Will get the last messages matching the subjects.
         *
         * @param subjects the subjects to get the last messages for
         * @return Builder
         */
        public Builder multiLastForSubjects(Collection<String> subjects) {
            this.multiLastFor.clear();
            this.multiLastFor.addAll(subjects);
            return this;
        }

        /**
         * Only return messages up to this sequence.
         * If not set, will be last sequence for the stream.
         *
         * @param upToSequence the maximum message sequence to return results for
         * @return Builder
         */
        public Builder upToSequence(long upToSequence) {
            validateGtZero(upToSequence, "Up to sequence");
            this.upToSequence = upToSequence;
            return this;
        }

        /**
         * Only return messages up to this time.
         *
         * @param upToTime the maximum message time to return results for
         * @return Builder
         */
        public Builder upToTime(ZonedDateTime upToTime) {
            this.upToTime = upToTime;
            return this;
        }

        /**
         * Build the {@link MessageBatchGetRequest}.
         *
         * @return MessageBatchGetRequest
         */
        public MessageBatchGetRequest build() {
            return new MessageBatchGetRequest(this);
        }
    }
}
