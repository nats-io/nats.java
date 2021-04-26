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

import io.nats.client.support.JsPrefixManager;

import java.time.Duration;

/**
 * The JetStreamOptions class specifies the general options for JetStream.
 * Options are created using the  {@link JetStreamOptions.Builder Builder}.
 */
public class JetStreamOptions {

    public static final Duration DEFAULT_TIMEOUT = Options.DEFAULT_CONNECTION_TIMEOUT;
    public static final JetStreamOptions DEFAULT_JS_OPTIONS = new Builder().build();

    private final String prefix;
    private final Duration requestTimeout;
    private final boolean publishNoAck;

    private JetStreamOptions(String prefix, Duration requestTimeout, boolean publishNoAck) {
        this.prefix = prefix;
        this.requestTimeout = requestTimeout;
        this.publishNoAck = publishNoAck;
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
     * Gets the whether the publish no ack flag was set
     * @return the flag
     */
    public boolean isPublishNoAck() {
        return publishNoAck;
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
    public static Builder builder(JetStreamOptions jso) {
        return new Builder(jso);
    }

    /**
     * Get an instance of JetStreamOptions with all defaults
     * @return the configuration
     */
    public static JetStreamOptions defaultOptions() {
        return DEFAULT_JS_OPTIONS;
    }

    /**
     * JetStreamOptions can be created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     */
    public static class Builder {

        private String prefix;
        private Duration requestTimeout;
        private boolean publishNoAck;

        public Builder() {}

        public Builder(JetStreamOptions jso) {
            if (jso != null) {
                this.prefix = jso.prefix;
                this.requestTimeout = jso.requestTimeout;
                this.publishNoAck = jso.publishNoAck;
            }
        }

        /**
         * Sets the request timeout for JetStream API calls.
         * @param requestTimeout the duration to wait for responses.
         * @return the builder
         */
        public Builder requestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }

        /**
         * Sets the prefix for JetStream subjects. A prefix can be used in conjunction with
         * user permissions to restrict access to certain JetStream instances.  This must
         * match the prefix used in the server.
         * @param prefix the JetStream prefix
         * @return the builder.
         */
        public Builder prefix(String prefix) {
            this.prefix = prefix; // validated in prefix manager
            return this;
        }

        /**
         * Sets whether the streams in use by contexts created with these options are no-ack streams.
         * @param publishNoAck how to treat publishes to the stream
         * @return the builder
         */
        public Builder publishNoAck(final boolean publishNoAck) {
            this.publishNoAck = publishNoAck;
            return this;
        }

        /**
         * Builds the JetStream options.
         * @return JetStream options
         */
        public JetStreamOptions build() {
            prefix = JsPrefixManager.addPrefix(prefix);
            this.requestTimeout = requestTimeout == null ? DEFAULT_TIMEOUT : requestTimeout;
            return new JetStreamOptions(prefix, requestTimeout, publishNoAck);
        }
    }
}
