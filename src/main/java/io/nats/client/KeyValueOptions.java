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

/**
 * The KeyValueOptions class specifies the general options for KeyValueO.
 * Options are created using the {@link KeyValueOptions.Builder Builder}.
 */
public class KeyValueOptions extends FeatureOptions {

    private KeyValueOptions(Builder b) {
        super(b);
    }

    /**
     * Creates a builder for the publish options.
     * @return the builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a builder to copy the options.
     * @param kvo an existing KeyValueOptions
     * @return a stream configuration builder
     */
    public static Builder builder(KeyValueOptions kvo) {
        return new Builder(kvo);
    }

    /**
     * Creates a builder to copy the options.
     * @param jso an existing JetStreamOptions
     * @return a stream configuration builder
     */
    public static Builder builder(JetStreamOptions jso) {
        return new Builder().jetStreamOptions(jso);
    }

    /**
     * KeyValueOptions can be created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     */
    public static class Builder extends FeatureOptions.Builder<Builder, KeyValueOptions> {

        @Override
        protected Builder getThis() {
            return this;
        }

        public Builder() {
            super();
        }

        public Builder(KeyValueOptions kvo) {
            super(kvo);
        }

        /**
         * Builds the KeyValue Options.
         * @return KeyValue Options
         */
        public KeyValueOptions build() {
            return new KeyValueOptions(this);
        }
    }
}
