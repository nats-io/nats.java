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

import static io.nats.client.support.Validator.validateJetStreamPrefix;

/**
 * The JetStreamOptions class specifies the general options for JetStream.
 * Options are created using the  {@link JetStreamOptions.Builder Builder}.
 */
public class JetStreamOptions {

    private final String prefix;
    private final Duration requestTimeout;
    private final boolean direct;

    protected JetStreamOptions(String prefix, Duration requestTimeout, boolean direct) {
        this.prefix = prefix;
        this.requestTimeout = requestTimeout;
        this.direct = direct;
    }

    /**
     * Gets the request timeout the stream.
     * @return the name of the stream.
     */
    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    /**
     * Gets the prefix for this JetStream context. A prefix can be used in conjunction with
     * user permissions to restrict access to certain JetStream instances.
     * @return the prefix to set.
     */
    public String getPrefix() {
        return prefix;
    }

    /**
     * Gets direct mode of the client.  Disabled by default. 
     * @return true if in direct mode, false otherwise.
     */
    public boolean isDirectMode() {
        return direct;
    }

    /**
     * Creates a builder for the publish options.
     * @return the builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * SubscribeOptions can be created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     */
    public static class Builder {

        private String prefix = null;
        private Duration requestTimeout = Options.DEFAULT_CONNECTION_TIMEOUT;
        boolean direct = false;
        
        /**
         * Sets the request timeout for JetStream API calls.
         * @param timeout the duration to wait for responses.
         * @return the builder
         */
        public Builder requestTimeout(Duration timeout) {
            this.requestTimeout = timeout;
            return this;
        }

        /**
         * Sets the prefix for JetStream subjects. A prefix can be used in conjunction with
         * user permissions to restrict access to certain JetStream instances.  This must
         * match the prefix used in the server.
         * @param value the JetStream prefix
         * @return the builder.
         */
        public Builder prefix(String value) {
            this.prefix = validateJetStreamPrefix(value);
            return this;
        }

        /**
         * Sets direct mode for the client.  It is disabled by default.
         * @param value true enables direct mode, false disables.
         * @return the builder.
         */
        public Builder direct(boolean value) {
            this.direct = value;
            return this;
        }        

        /**
         * Builds the JetStream options.
         * @return JetStream options
         */
        public JetStreamOptions build() {
            return new JetStreamOptions(prefix, requestTimeout, direct);
        }
    }

}
