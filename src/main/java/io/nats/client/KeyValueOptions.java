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

import java.time.Duration;

import static io.nats.client.support.Validator.ensureEndsWithDot;
import static io.nats.client.support.Validator.validatePrefixOrDomain;

/**
 * The KeyValueOptions class specifies the general options for KeyValueO.
 * Options are created using the {@link KeyValueOptions.Builder Builder}.
 */
public class KeyValueOptions {

    public static final Duration DEFAULT_TIMEOUT = Options.DEFAULT_CONNECTION_TIMEOUT;
    public static final KeyValueOptions DEFAULT_JS_OPTIONS = new Builder().build();

    private final String featurePrefix;
    private final JetStreamOptions jso;

    private KeyValueOptions(String featurePrefix, JetStreamOptions jso) {
        this.featurePrefix = featurePrefix;
        this.jso = jso;
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
     * Creates a builder for the publish options.
     * @return the builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a builder to copy the options.
     * @param jso an existing JetStreamOptions
     * @return a stream configuration builder
     */
    public static Builder builder(KeyValueOptions jso) {
        return new Builder(jso);
    }

    /**
     * KeyValueOptions can be created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     */
    public static class Builder {

        private String featurePrefix;
        private JetStreamOptions jso;

        public Builder() {}

        public Builder(KeyValueOptions kvo) {
            if (kvo != null) {
                this.featurePrefix = kvo.featurePrefix;
                this.jso = kvo.jso;
            }
        }

        /**
         * Sets the prefix for subject in features such as KeyValue.
         * @param featurePrefix the feature prefix
         * @return the builder.
         */
        public Builder featurePrefix(String featurePrefix) {
            this.featurePrefix = ensureEndsWithDot(validatePrefixOrDomain(featurePrefix, "Feature Prefix", false));
            return this;
        }

        /**
         * Sets the JetStreamOptions.
         * @param jso the JetStreamOptions
         * @return the builder.
         */
        public Builder jetStreamOptions(JetStreamOptions jso) {
            this.jso = jso;
            return this;
        }

        /**
         * Builds the JetStream options.
         * @return JetStream options
         */
        public KeyValueOptions build() {
            return new KeyValueOptions(featurePrefix, jso);
        }
    }
}
