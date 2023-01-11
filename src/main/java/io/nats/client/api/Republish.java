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

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.*;
import static io.nats.client.support.JsonValueUtils.getMappedBoolean;
import static io.nats.client.support.JsonValueUtils.getMappedString;

/**
 * Republish directives to consider
 */
public class Republish implements JsonSerializable {
    private final String source;
    private final String destination;
    private final boolean headersOnly;

    static Republish optionalInstance(JsonValue vRepublish) {
        return vRepublish == null ? null :
            new Republish(
                getMappedString(vRepublish, SRC),
                getMappedString(vRepublish, DEST),
                getMappedBoolean(vRepublish, HEADERS_ONLY));
    }

    /**
     * Construct a 'republish' object
     * @param source the Published Subject-matching filter
     * @param destination the RePublish Subject template
     * @param headersOnly Whether to RePublish only headers (no body)
     */
    public Republish(String source, String destination, boolean headersOnly) {
        this.source = source;
        this.destination = destination;
        this.headersOnly = headersOnly;
    }

    public String getSource() {
        return source;
    }

    public String getDestination() {
        return destination;
    }

    public boolean isHeadersOnly() {
        return headersOnly;
    }

    public String toJson() {
        StringBuilder sb = beginJson();
        addField(sb, SRC, source);
        addField(sb, DEST, destination);
        addFldWhenTrue(sb, HEADERS_ONLY, headersOnly);
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
        private boolean headersOnly;

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
         * Set the RePublish Subject template
         * @param destination the destination
         * @return the builder
         */
        public Builder destination(String destination) {
            this.destination = destination;
            return this;
        }

        /**
         * set Whether to RePublish only headers (no body)
         * @param headersOnly the flag
         * @return Builder
         */
        public Builder headersOnly(Boolean headersOnly) {
            this.headersOnly = headersOnly;
            return this;
        }

        /**
         * Build a Placement object
         * @return the Placement
         */
        public Republish build() {
            Validator.required(source, "Source");
            Validator.required(destination, "Destination");
            return new Republish(source, destination, headersOnly);
        }
    }
}
