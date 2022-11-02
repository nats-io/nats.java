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

import static io.nats.client.support.NatsConstants.DOT;
import static io.nats.client.support.NatsJetStreamConstants.*;
import static io.nats.client.support.Validator.ensureEndsWithDot;
import static io.nats.client.support.Validator.validatePrefixOrDomain;

/**
 * The JetStreamOptions class specifies the general options for JetStream.
 * Options are created using the  {@link JetStreamOptions.Builder Builder}.
 */
public class JetStreamOptions {

    public static final Duration DEFAULT_TIMEOUT = Options.DEFAULT_CONNECTION_TIMEOUT;
    public static final JetStreamOptions DEFAULT_JS_OPTIONS = new Builder().build();

    private final String jsPrefix;
    private final Duration requestTimeout;
    private final boolean publishNoAck;
    private final boolean defaultPrefix;
    private final boolean optOut290ConsumerCreate;

    private JetStreamOptions(Builder b) {
        if (b.jsPrefix == null) {
            defaultPrefix = true;
            this.jsPrefix = DEFAULT_API_PREFIX;
        }
        else {
            defaultPrefix = false;
            this.jsPrefix = b.jsPrefix;
        }
        this.requestTimeout = b.requestTimeout;
        this.publishNoAck = b.publishNoAck;
        this.optOut290ConsumerCreate = b.optOut290ConsumerCreate;
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
     * @return the prefix.
     */
    public String getPrefix() {
        return jsPrefix;
    }

    /**
     * Returns true if the prefix for the options is the default prefix.
     * @return the true for default prefix.
     */
    public boolean isDefaultPrefix() {
        return defaultPrefix;
    }

    /**
     * Gets the whether the publish no ack flag was set
     * @return the flag
     */
    public boolean isPublishNoAck() {
        return publishNoAck;
    }

    /**
     * Gets whether the opt-out of the server v2.9.0 consumer create api is set
     * @return the flag
     */
    public boolean isOptOut290ConsumerCreate() {
        return optOut290ConsumerCreate;
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
     * @param jso an existing JetStreamOptions
     * @return a JetStreamOptions builder
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

        private String jsPrefix;
        private Duration requestTimeout;
        private boolean publishNoAck;
        private boolean optOut290ConsumerCreate;

        public Builder() {}

        public Builder(JetStreamOptions jso) {
            if (jso != null) {
                if (jso.isDefaultPrefix()) {
                    this.jsPrefix = null;
                }
                else {
                    this.jsPrefix = jso.jsPrefix;
                }
                this.requestTimeout = jso.requestTimeout;
                this.publishNoAck = jso.publishNoAck;
                this.optOut290ConsumerCreate = jso.optOut290ConsumerCreate;
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
         * user permissions to restrict access to certain JetStream instances. This must
         * match the prefix used in the server.
         * @param prefix the JetStream prefix
         * @return the builder.
         */
        public Builder prefix(String prefix) {
            jsPrefix = ensureEndsWithDot(validatePrefixOrDomain(prefix, "Prefix", false));
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
        public Builder domain(String domain) {
            String valid = validatePrefixOrDomain(domain, "Domain", false);
            jsPrefix = valid == null ? null : convertDomainToPrefix(valid) + DOT;
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
         * Set whether to opt-out of the server v2.9.0 consumer create api. Default is false (opt-in)
         * @param optOut the opt-out flag
         * @return the builder
         */
        public Builder optOut290ConsumerCreate(boolean optOut) {
            this.optOut290ConsumerCreate = optOut;
            return this;
        }

        /**
         * Builds the JetStream options.
         * @return JetStream options
         */
        public JetStreamOptions build() {
            this.requestTimeout = requestTimeout == null ? DEFAULT_TIMEOUT : requestTimeout;
            return new JetStreamOptions(this);
        }
    }

    public static String convertDomainToPrefix(String domain) {
        return PREFIX_DOLLAR_JS_DOT
            + ensureEndsWithDot(validatePrefixOrDomain(domain, "Domain", false))
            + PREFIX_API;
    }
}
