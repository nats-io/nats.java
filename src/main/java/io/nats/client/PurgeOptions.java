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

import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonUtils;
import org.jspecify.annotations.NonNull;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;
import static io.nats.client.support.Validator.validateSubjectStrict;

/**
 * The PurgeOptions class specifies the options for purging a stream
 */
public class PurgeOptions implements JsonSerializable {

    protected final String subject;
    protected final long seq;
    protected final long keep;

    private PurgeOptions(String subject, long seq, long keep) {
        this.subject = subject;
        this.seq = seq;
        this.keep = keep;
    }

    @Override
    @NonNull
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, FILTER, subject);
        JsonUtils.addField(sb, SEQ, seq);
        JsonUtils.addField(sb, KEEP, keep);
        return endJson(sb).toString();
    }

    /**
     * Get the subject for the Purge Options
     * @return the subject
     */
    public String getSubject() {
        return subject;
    }

    /**
     * Get the upper bound sequence for the Purge Options
     * @return the upper bound sequence
     */
    public long getSequence() {
        return seq;
    }

    /**
     * Get the max number of messages to keep for the Purge Options
     * @return the max number of messages to keep
     */
    public long getKeep() {
        return keep;
    }

    /**
     * Creates a builder for the purge options
     * @return a purge options builder
     */
    public static PurgeOptions.Builder builder() {
        return new Builder();
    }

    /**
     * Creates a completed Purge Options for just a subject
     * @param subject the subject to purge
     * @return a purge options for a subject
     */
    public static PurgeOptions subject(String subject) {
        return new Builder().subject(subject).build();
    }

    /**
     * Builder class for PurgeOptions
     */
    public static class Builder {
        private String subject;
        private long seq = -1;
        private long keep = -1;

        /**
         * Construct a builder instance
         */
        public Builder() {}

        /**
         * Set the subject to filter the purge. Wildcards allowed.
         * @param subject the subject
         * @return the builder
         */
        public Builder subject(final String subject) {
            this.subject = validateSubjectStrict(subject, false);
            return this;
        }

        /**
         * Set upper-bound sequence for messages to be deleted
         * @param seq the upper-bound sequence
         * @return the builder
         */
        public Builder sequence(final long seq) {
            this.seq = seq;
            return this;
        }

        /**
         * set the max number of messages to keep
         * @param keep the max number of messages to keep
         * @return the builder
         */
        public Builder keep(final long keep) {
            this.keep = keep;
            return this;
        }

        /**
         * Build the PurgeOptions
         * @return the built PurgeOptions
         */
        public PurgeOptions build() {
            if (seq > 0 && keep > 0) {
                throw new IllegalArgumentException("seq and keep are mutually exclusive.");
            }

            return new PurgeOptions(subject, seq, keep);
        }
    }
}
