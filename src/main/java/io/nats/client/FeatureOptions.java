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

import static io.nats.client.support.Validator.ensureEndsWithDot;
import static io.nats.client.support.Validator.validatePrefixOrDomain;

/**
 * The FeatureOptions class specifies the general options for features.
 * Options are created using the {@link FeatureOptions.Builder Builder}.
 */
public class FeatureOptions {

    private final String featurePrefix;
    private final JetStreamOptions jso;

    @SuppressWarnings("rawtypes") // Don't need the type of the builder to get it's vars
    protected FeatureOptions(Builder b) {
        featurePrefix = b.featurePrefix;
        jso = b.jso;
    }

    /**
     * Gets the feature [subject] prefix.
     * @return the prefix.
     */
    public String getFeaturePrefix() {
        return featurePrefix;
    }

    /**
     * Gets the JetStream options for a KeyValue store
     * @return the options
     */
    public JetStreamOptions getJetStreamOptions() {
        return jso;
    }

    /**
     * KeyValueOptions can be created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     */
    protected static abstract class Builder<B, FO> {

        private String featurePrefix;
        private JetStreamOptions jso;

        protected abstract B getThis();

        protected Builder(FeatureOptions oso) {
            if (oso != null) {
                this.featurePrefix = oso.featurePrefix;
                this.jso = oso.jso;
            }
        }

        /**
         * Sets the prefix for subject in features such as KeyValue.
         * @param featurePrefix the feature prefix
         * @return the builder.
         */
        public B featurePrefix(String featurePrefix) {
            this.featurePrefix = ensureEndsWithDot(validatePrefixOrDomain(featurePrefix, "Feature Prefix", false));
            return getThis();
        }

        /**
         * Sets the JetStreamOptions.
         * @param jso the JetStreamOptions
         * @return the builder.
         */
        public B jetStreamOptions(JetStreamOptions jso) {
            this.jso = jso;
            return getThis();
        }

        /**
         * Builds the Feature options.
         * @return Feature options
         */
        public abstract FO build();
    }
}
