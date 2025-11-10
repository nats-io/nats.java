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

import io.nats.client.support.JsonValue;

/**
 * Mirror Information. Maintains a 1:1 mirror of another stream with name matching this property.
 * When a mirror is configured subjects and sources must be empty.
 */
public class Mirror extends SourceBase {

    static Mirror optionalInstance(JsonValue vMirror) {
        return vMirror == null ? null : new Mirror(vMirror);
    }

    Mirror(JsonValue vMirror) {
        super(vMirror);
    }

    Mirror(Builder b) {
        super(b);
    }

    /**
     * Create an instance of the Mirror.Builder
     * @return the instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Create an instance of the Mirror.Builder with a copy of all the information in the source mirror
     * @param mirror the source mirror
     * @return the instance
     */
    public static Builder builder(Mirror mirror) {
        return new Builder(mirror);
    }

    /**
     * The builder for a Mirror
     */
    public static class Builder extends SourceBaseBuilder<Builder> {
        @Override
        Builder getThis() {
            return this;
        }

        /**
         * Construct a builder
         */
        public Builder() {}

        /**
         * Construct a builder with a copy of all the information in the source mirror
         * @param mirror the source mirror
         */
        public Builder(Mirror mirror) {
            super(mirror);
        }

        /**
         * Build the mirror from the builder
         * @return the Mirror
         */
        public Mirror build() {
            return new Mirror(this);
        }
    }
}
