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

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.*;

/**
 * Object used to make a request for message batch get requests.
 */
public class MessageBatchGetRequest implements JsonSerializable {

    private final int batch;
    private final int maxBytes;
    private final long minSequence;
    private final ZonedDateTime startTime;
    private final String nextBySubject;
    private final List<String> multiLastBySubjects;
    private final long upToSequence;
    private final ZonedDateTime upToTime;

    MessageBatchGetRequest(Builder b) {
        this.batch = b.batch;
        this.maxBytes = b.maxBytes;
        this.minSequence = b.minSequence;
        this.startTime = b.startTime;
        this.nextBySubject = b.nextBySubject;
        this.multiLastBySubjects = b.multiLastBySubjects;
        this.upToSequence = b.upToSequence;
        this.upToTime = b.upToTime;
    }

    /**
     * Maximum amount of messages to be returned for this request.
     * @return batch size
     */
    public int getBatch() {
        return batch;
    }

    /**
     * Maximum amount of returned bytes for this request.
     * Limits the amount of returned messages to not exceed this.
     * @return maximum bytes
     */
    public int getMaxBytes() {
        return maxBytes;
    }

    /**
     * Minimum sequence for returned messages.
     * All returned messages will have a sequence equal to or higher than this.
     * @return minimum message sequence
     */
    public long getMinSequence() {
        return minSequence;
    }

    /**
     * Minimum start time for returned messages.
     * All returned messages will have a start time equal to or higher than this.
     * @return minimum message start time
     */
    public ZonedDateTime getStartTime() {
        return startTime;
    }

    /**
     * Subject used to filter messages that should be returned.
     * @return the subject to filter
     */
    public String getNextBySubject() {
        return nextBySubject;
    }

    /**
     * Subjects filter used, these can include wildcards.
     * Will get the last messages matching the subjects.
     * @return the subjects to get the last messages for
     */
    public List<String> getMultiLastBySubjects() {
        return multiLastBySubjects;
    }

    /**
     * Only return messages up to this sequence.
     * @return the maximum message sequence to return results for
     */
    public long getUpToSequence() {
        return upToSequence;
    }

    /**
     * Only return messages up to this time.
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
        addField(sb, START_TIME, startTime);
        addField(sb, NEXT_BY_SUBJECT, nextBySubject);
        addStrings(sb, MULTI_LAST, multiLastBySubjects);
        addField(sb, UP_TO_SEQ, upToSequence);
        addField(sb, UP_TO_TIME, upToTime);
        if (minSequence < 1 && (maxBytes > 0 || upToSequence > 0 || upToTime != null)) {
            addField(sb, SEQ, 1);
        }
        else {
            addField(sb, SEQ, minSequence);
        }
        return endJson(sb).toString();
    }

    /**
     * Creates a builder for the request.
     * @return Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a builder for the request.
     * @param req the {@link MessageBatchGetRequest}
     * @return Builder
     */
    public static Builder builder(MessageBatchGetRequest req) {
        return req == null ? new Builder() : new Builder(req);
    }

    /**
     * {@link MessageBatchGetRequest} is created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     * <p>{@code MessageBatchGetRequest.builder().build()} will create a default {@link MessageBatchGetRequest}.
     */
    public static class Builder {
        private int batch = -1;
        private int maxBytes = -1;
        private long minSequence = -1;
        private ZonedDateTime startTime = null;
        private String nextBySubject = null;
        private List<String> multiLastBySubjects = new ArrayList<>();
        private long upToSequence = -1;
        private ZonedDateTime upToTime = null;

        /**
         * Construct the builder
         */
        public Builder() {
        }

        /**
         * Construct the builder and initialize values with the existing {@link MessageBatchGetRequest}
         * @param req the {@link MessageBatchGetRequest} to clone
         */
        public Builder(MessageBatchGetRequest req) {
            if (req != null) {
                this.batch = req.batch;
                this.maxBytes = req.maxBytes;
                this.minSequence = req.minSequence;
                this.startTime = req.startTime;
                this.nextBySubject = req.nextBySubject;
                this.multiLastBySubjects = req.multiLastBySubjects;
                this.upToSequence = req.upToSequence;
                this.upToTime = req.upToTime;
            }
        }

        /**
         * Set the maximum amount of messages to be returned for this request.
         * @param batch the batch size
         * @return Builder
         */
        public Builder batch(int batch) {
            this.batch = batch < 1 ? -1 : batch;
            return this;
        }

        /**
         * Maximum amount of returned bytes for this request.
         * Limits the amount of returned messages to not exceed this.
         * @param maxBytes the maximum bytes
         * @return Builder
         */
        public Builder maxBytes(int maxBytes) {
            this.maxBytes = maxBytes < 1 ? -1 : maxBytes;
            return this;
        }

        /**
         * Minimum sequence for returned messages.
         * All returned messages will have a sequence equal to or higher than this.
         * @param sequence the minimum message sequence
         * @return Builder
         */
        public Builder minSequence(long sequence) {
            this.minSequence = sequence < 1 ? -1 : sequence;
            return this;
        }

        /**
         * Minimum start time for returned messages.
         * All returned messages will have a start time equal to or higher than this.
         * @param startTime the minimum message start time
         * @return Builder
         */
        public Builder startTime(ZonedDateTime startTime) {
            this.startTime = startTime;
            return this;
        }

        /**
         * Subject used to filter messages that should be returned.
         * @param nextBySubject the subject to filter
         * @return Builder
         */
        public Builder nextBySubject(String nextBySubject) {
            if (!multiLastBySubjects.isEmpty()) {
                throw new IllegalArgumentException("nextBySubject cannot be used when multiLastBySubjects is used.");
            }
            this.nextBySubject = nextBySubject;
            return this;
        }

        /**
         * Subjects filter used, these can include wildcards.
         * Will get the last messages matching the subjects.
         * @param subjects the subjects to get the last messages for
         * @return Builder
         */
        public Builder multiLastBySubjects(String... subjects) {
            if (nextBySubject != null) {
                throw new IllegalArgumentException("nextBySubject cannot be used when multiLastBySubjects is used.");
            }
            this.multiLastBySubjects.clear();
            if (subjects != null && subjects.length > 0) {
                this.multiLastBySubjects.addAll(Arrays.asList(subjects));
            }
            return this;
        }

        /**
         * Subjects filter used, these can include wildcards.
         * Will get the last messages matching the subjects.
         * @param subjects the subjects to get the last messages for
         * @return Builder
         */
        public Builder multiLastBySubjects(Collection<String> subjects) {
            if (nextBySubject != null) {
                throw new IllegalArgumentException("nextBySubject cannot be used when multiLastBySubjects is used.");
            }
            this.multiLastBySubjects.clear();
            if (subjects != null && !subjects.isEmpty()) {
                this.multiLastBySubjects.addAll(subjects);
            }
            return this;
        }

        /**
         * Only return messages up to this sequence.
         * If not set, will be last sequence for the stream.
         * @param upToSequence the maximum message sequence to return results for
         * @return Builder
         */
        public Builder upToSequence(long upToSequence) {
            this.upToSequence = upToSequence < 1 ? -1 : upToSequence;
            return this;
        }

        /**
         * Only return messages up to this time.
         * @param upToTime the maximum message time to return results for
         * @return Builder
         */
        public Builder upToTime(ZonedDateTime upToTime) {
            this.upToTime = upToTime;
            return this;
        }

        /**
         * Build the {@link MessageBatchGetRequest}.
         * @return MessageBatchGetRequest
         */
        public MessageBatchGetRequest build() {
            return new MessageBatchGetRequest(this);
        }
    }
}
