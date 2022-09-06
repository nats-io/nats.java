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

import java.time.Duration;

/**
 * The FeatureOptions is a base class of general options for features.
 */
public abstract class FeatureOptions {

    private final JetStreamOptions jso;

    @SuppressWarnings("rawtypes") // Don't need the type of the builder to get its vars
    protected FeatureOptions(Builder b) {
        jso = b.jsoBuilder.build();
    }

    /**
     * Gets the JetStream options
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

        private JetStreamOptions.Builder jsoBuilder;

        protected abstract B getThis();
        
        protected Builder() {
            jsoBuilder = JetStreamOptions.builder();
        }
        
        protected Builder(FeatureOptions oso) {
            if (oso != null) {
                jsoBuilder = JetStreamOptions.builder(oso.jso);
            }
            else {
                jsoBuilder = JetStreamOptions.builder();
            }
        }

        /**
         * Sets the JetStreamOptions.
         * @param jso the JetStreamOptions
         * @return the builder.
         */
        public B jetStreamOptions(JetStreamOptions jso) {
            jsoBuilder = JetStreamOptions.builder(jso);
            return getThis();
        }

        /**
         * Sets the request timeout for JetStream API calls.
         * @param requestTimeout the duration to wait for responses.
         * @return the builder
         */
        public B jsRequestTimeout(Duration requestTimeout) {
            jsoBuilder.requestTimeout(requestTimeout);
            return getThis();
        }

        /**
         * Sets the prefix for JetStream subjects. A prefix can be used in conjunction with
         * user permissions to restrict access to certain JetStream instances. This must
         * match the prefix used in the server.
         * @param prefix the JetStream prefix
         * @return the builder.
         */
        public B jsPrefix(String prefix) {
            jsoBuilder.prefix(prefix);
            return getThis();
        }

        /**
         * Sets the domain for JetStream subjects, creating a standard prefix from that domain
         * in the form $JS.(domain).API.
         * A domain can be used in conjunction with user permissions to restrict access to certain JetStream instances.
         * This must match the domain used in the server.
         * @param domain the JetStream domain
         * @return the builder.
         */
        public B jsDomain(String domain) {
            jsoBuilder.domain(domain);
            return getThis();
        }

        /**
         * Builds the Feature options.
         * @return Feature options
         */
        public abstract FO build();
    }
}
