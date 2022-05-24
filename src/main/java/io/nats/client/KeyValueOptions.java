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

/**
 * The KeyValueOptions class specifies the general options for KeyValueO.
 * Options are created using the {@link KeyValueOptions.Builder Builder}.
 */
public class KeyValueOptions {

    private final JetStreamOptions jso;

    private KeyValueOptions(JetStreamOptions jso) {
        this.jso = jso;
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
    public static class Builder {

        private JetStreamOptions.Builder jsoBuilder;

        public Builder() {
            this(null);
        }

        public Builder(KeyValueOptions kvo) {
            if (kvo == null) {
                jsoBuilder = new JetStreamOptions.Builder();
            }
            else {
                jsoBuilder = new JetStreamOptions.Builder(kvo.jso);
            }
        }

        /**
         * Sets the JetStreamOptions.
         * @param jso the JetStreamOptions
         * @return the builder.
         */
        public Builder jetStreamOptions(JetStreamOptions jso) {
            jsoBuilder = new JetStreamOptions.Builder(jso);
            return this;
        }

        /**
         * Sets the request timeout for JetStream API calls.
         * @param requestTimeout the duration to wait for responses.
         * @return the builder
         */
        public Builder jsRequestTimeout(Duration requestTimeout) {
            jsoBuilder.requestTimeout(requestTimeout);
            return this;
        }

        /**
         * Sets the prefix for JetStream subjects. A prefix can be used in conjunction with
         * user permissions to restrict access to certain JetStream instances. This must
         * match the prefix used in the server.
         * @param prefix the JetStream prefix
         * @return the builder.
         */
        public Builder jsPrefix(String prefix) {
            jsoBuilder.prefix(prefix);
            return this;
        }

        /**
         * Sets the domain for JetStream subjects, creating a standard prefix from that domain
         * in the form $JS.(domain).API.
         * A domain can be used in conjunction with user permissions to restrict access to certain JetStream instances.
         * This must match the domain used in the server.
         * @param domain the JetStream domain
         * @return the builder.
         */
        public Builder jsDomain(String domain) {
            jsoBuilder.domain(domain);
            return this;
        }

        /**
         * Builds the JetStream options.
         * @return JetStream options
         */
        public KeyValueOptions build() {
            return new KeyValueOptions(jsoBuilder.build());
        }
    }
}
