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
import io.nats.client.support.JsonValueUtils;

import java.util.List;

/**
 * Source Information
 */
public class Source extends SourceBase {

    static List<Source> optionalListOf(JsonValue vSources) {
        return JsonValueUtils.optionalListOf(vSources, Source::new);
    }

    Source(JsonValue vSource) {
        super(vSource);
    }

    Source(Builder b) {
        super(b);
    }

    /**
     * Get an instance of the builder
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Get an instance of the builder copying an existing source
     * @param source the source
     * @return the builder
     */
    public static Builder builder(Source source) {
        return new Builder(source);
    }

    /**
     * The builder for a Source
     */
    public static class Builder extends SourceBaseBuilder<Builder> {
        @Override
        Builder getThis() {
            return this;
        }

        /**
         * Construct an instance of the builder
         */
        public Builder() {}

        /**
         * Construct an instance of the builder copying an existing source
         * @param source the source
         */
        public Builder(Source source) {
            super(source);
        }

        /**
         * Build a Source
         * @return the Source
         */
        public Source build() {
            return new Source(this);
        }
    }
}
