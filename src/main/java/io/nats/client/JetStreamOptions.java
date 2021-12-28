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
import static io.nats.client.support.Validator.emptyAsNull;
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
    private final String featurePrefix;

    private JetStreamOptions(String inJsPrefix, String featurePrefix, Duration requestTimeout, boolean publishNoAck) {
        if (inJsPrefix == null) {
            defaultPrefix = true;
            this.jsPrefix = DEFAULT_API_PREFIX;
        }
        else {
            defaultPrefix = false;
            this.jsPrefix = inJsPrefix;
        }
        this.requestTimeout = requestTimeout;
        this.publishNoAck = publishNoAck;
        this.featurePrefix = featurePrefix;
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
     * Returns true if the prefix for this options is the default prefix.
     * @return the true for default prefix.
     */
    public boolean isDefaultPrefix() {
        return defaultPrefix;
    }

    /**
     * Gets the feature [subject] prefix.
     * @return the prefix.
     */
    public String getFeaturePrefix() {
        return featurePrefix;
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

        private String jsPrefix;
        private String featurePrefix;
        private Duration requestTimeout;
        private boolean publishNoAck;

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
            String temp = emptyAsNull(prefix);
            if (temp != null) {
                jsPrefix = validateJsPrefix(temp);
            }
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
            String temp = emptyAsNull(domain);
            if (temp != null) {
                jsPrefix = validateJsDomain(temp);
            }
            return this;
        }

        /**
         * Sets the prefix for subject in features such as KeyValue.
         * @param featurePrefix the feature prefix
         * @return the builder.
         */
        public Builder featurePrefix(String featurePrefix) {
            this.featurePrefix = validateFeaturePrefix(emptyAsNull(featurePrefix));
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
            this.requestTimeout = requestTimeout == null ? DEFAULT_TIMEOUT : requestTimeout;
            return new JetStreamOptions(jsPrefix, featurePrefix, requestTimeout, publishNoAck);
        }

        private String validateJsPrefix(String prefix) {
            String valid = validatePrefixOrDomain(prefix, "Prefix", true);
            return valid.endsWith(DOT) ? valid : valid + DOT;
        }

        private String validateJsDomain(String domain) {
            String valid = validatePrefixOrDomain(domain, "Domain", true);
            if (valid.endsWith(DOT)) {
                return PREFIX_DOLLAR_JS_DOT + valid + PREFIX_API_DOT;
            }
            return PREFIX_DOLLAR_JS_DOT + valid + DOT + PREFIX_API_DOT;
        }

        private String validateFeaturePrefix(String prefix) {
            String valid = validatePrefixOrDomain(prefix, "Feature Prefix", false);
            return valid == null ? null : valid.endsWith(DOT) ? valid : valid + DOT;
        }
    }
}
