// Copyright 2022 The NATS Authors
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

import static io.nats.client.support.ApiConstants.DELETED_DETAILS;
import static io.nats.client.support.ApiConstants.SUBJECTS_FILTER;
import static io.nats.client.support.JsonUtils.*;
import static io.nats.client.support.NatsConstants.GREATER_THAN;
import static io.nats.client.support.Validator.emptyAsNull;

/**
 * Object used to make a request for special stream info requests
 */
public class StreamInfoOptions implements JsonSerializable {
    private final String subjectsFilter;
    private final boolean deletedDetails;

    private StreamInfoOptions(String subjectsFilter, boolean deletedDetails) {
        this.subjectsFilter = subjectsFilter;
        this.deletedDetails = deletedDetails;
    }

    public String getSubjectsFilter() {
        return subjectsFilter;
    }

    public boolean isDeletedDetails() {
        return deletedDetails;
    }

    /**
     * Create options that get subject information, filtering for subjects. Wildcards are allowed.
     * @param subjectsFilter the subject filter. &gt; is equivalent to all
     * @return the StreamInfoOptions object
     */
    public static StreamInfoOptions filterSubjects(String subjectsFilter) {
        return new Builder().filterSubjects(subjectsFilter).build();
    }

    /**
     * Create options that get subject information, filtering for all subjects.
     * @return the StreamInfoOptions object
     */
    public static StreamInfoOptions allSubjects() {
        return new Builder().allSubjects().build();
    }

    /**
     * Create options that get deleted details.
     * @return the StreamInfoOptions object
     */
    public static StreamInfoOptions deletedDetails() {
        return new Builder().deletedDetails().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        addField(sb, SUBJECTS_FILTER, subjectsFilter);
        addFldWhenTrue(sb, DELETED_DETAILS, deletedDetails);
        return endJson(sb).toString();
    }

    /**
     * StreamInfoOptions is created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     *
     * <p>{@code new StreamInfoOptions.Builder().build()} will create a new StreamInfoOptions.
     *
     */
    public static class Builder {
        private String subjectsFilter;
        private boolean deletedDetails;

        /**
         * Default Builder
         */
        public Builder() {}

        /**
         * Set the subjects filter, which turns on getting subject info.
         * Setting the filter to &gt; is the same as all subjects
         * Setting the filter to null clears the filter and turns off getting subject info
         * @param subjectsFilter the
         * @return the builder
         */
        public Builder filterSubjects(String subjectsFilter) {
            this.subjectsFilter = emptyAsNull(subjectsFilter);
            return this;
        }

        /**
         * Set the subjects filter to &gt;, which turns on getting subject info.
         * @return the builder
         */
        public Builder allSubjects() {
            this.subjectsFilter = GREATER_THAN;
            return this;
        }

        /**
         * Turns on getting deleted details
         * @return the builder
         */
        public Builder deletedDetails() {
            this.deletedDetails = true;
            return this;
        }

        /**
         * Build the options
         * @return the StreamInfoOptions object
         */
        public StreamInfoOptions build() {
            return new StreamInfoOptions(subjectsFilter, deletedDetails);
        }
    }
}
