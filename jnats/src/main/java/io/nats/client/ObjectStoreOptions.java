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

package io.nats.client;

/**
 * The ObjectStoreOptions class specifies the general options for ObjectStore.
 * Options are created using the {@link ObjectStoreOptions.Builder Builder}.
 */
public class ObjectStoreOptions extends FeatureOptions {

    private ObjectStoreOptions(Builder b) {
        super(b);
    }

    /**
     * Creates a builder for the options.
     * @return the builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a builder to copy the options.
     * @param oso an existing ObjectStoreOptions
     * @return an ObjectStoreOptions builder
     */
    public static Builder builder(ObjectStoreOptions oso) {
        return new ObjectStoreOptions.Builder(oso);
    }

    /**
     * Creates a builder to copy the options.
     * @param jso an existing JetStreamOptions
     * @return an ObjectStoreOptions builder
     */
    public static Builder builder(JetStreamOptions jso) {
        return new Builder().jetStreamOptions(jso);
    }

    /**
     * ObjectStoreOptions can be created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     */
    public static class Builder extends FeatureOptions.Builder<Builder, ObjectStoreOptions> {

        @Override
        protected Builder getThis() {
            return this;
        }

        public Builder() {
            super();
        }

        public Builder(ObjectStoreOptions oso) {
            super(oso);
        }

        /**
         * Builds the ObjectStore options.
         * @return ObjectStore options
         */
        public ObjectStoreOptions build() {
            return new ObjectStoreOptions(this);
        }
    }
}
