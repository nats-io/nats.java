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
import io.nats.client.support.JsonValue;
import io.nats.client.support.Validator;

import static io.nats.client.support.ApiConstants.DEST;
import static io.nats.client.support.ApiConstants.SRC;
import static io.nats.client.support.JsonUtils.*;
import static io.nats.client.support.JsonValueUtils.readString;

/**
 * SubjectTransform
 */
public class SubjectTransform implements JsonSerializable {
    private final String source;
    private final String destination;

    static SubjectTransform optionalInstance(JsonValue vSubjectTransform) {
        return vSubjectTransform == null ? null : new SubjectTransform(vSubjectTransform);
    }

    SubjectTransform(JsonValue vSubjectTransform) {
        source = readString(vSubjectTransform, SRC);
        destination = readString(vSubjectTransform, DEST);
    }

    /**
     * Construct a 'SubjectTransform' object
     * @param source the Published Subject-matching filter
     * @param destination the SubjectTransform Subject template
     */
    public SubjectTransform(String source, String destination) {
        source = Validator.emptyAsNull(source);
        destination = Validator.emptyAsNull(destination);
        if (source == null) {
            if (destination == null) {
                throw new IllegalArgumentException("Source and/or destination is required.");
            }
            source = ">";
        }
        this.source = source;
        this.destination = destination;
    }

    /**
     * Get source, the Published Subject-matching filter
     * @return the source
     */
    public String getSource() {
        return source;
    }

    /**
     * Get destination, the SubjectTransform Subject template
     * @return the destination
     */
    public String getDestination() {
        return destination;
    }

    public String toJson() {
        StringBuilder sb = beginJson();
        addField(sb, SRC, source);
        addField(sb, DEST, destination);
        return endJson(sb).toString();
    }

    /**
     * Creates a builder for a placements object.
     * @return the builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Placement can be created using a Builder.
     */
    public static class Builder {
        private String source;
        private String destination;

        /**
         * Set the Published Subject-matching filter
         * @param source the source
         * @return the builder
         */
        public Builder source(String source) {
            this.source = source;
            return this;
        }
        /**
         * Set the SubjectTransform Subject template
         * @param destination the destination
         * @return the builder
         */
        public Builder destination(String destination) {
            this.destination = destination;
            return this;
        }

        /**
         * Build a Placement object
         * @return the Placement
         */
        public SubjectTransform build() {
            return new SubjectTransform(source, destination);
        }
    }
}
