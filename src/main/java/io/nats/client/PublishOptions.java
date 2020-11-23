// Copyright 2015-2018 The NATS Authors
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
import java.util.Properties;

/**
 * The PublishOptions class specifies the options for publishing with jetstream enabled servers.
 * Options are created using a {@link PublishOptions.Builder Builder}.
 */
public class PublishOptions {
    public static final Duration defaultTimeout = Duration.ofSeconds(2);
    public static final String unspecifiedStream = "not.set";

    private String stream = unspecifiedStream;
    private Duration streamTimeout = defaultTimeout;

    /**
     * Property used to configure a builder from a Properties object.
     */
    public static final String PROP_STREAM_NAME = Options.PFX + "publish.stream";

    /**
     * Property used to configure a builder from a Properties object..
     */
    public static final String PROP_PUBLISH_TIMEOUT = Options.PFX + "publish.timeout";       

    public PublishOptions(String stream, Duration timeout) {
        this.stream = stream;
        this.streamTimeout = timeout;
    }

    public PublishOptions(Duration timeout) {
        this.stream = null;
        this.streamTimeout = timeout;
    }

    public PublishOptions(String stream) {
        this.stream = stream;
    }

    /**
     * Gets the name of the stream.
     * @return the name of the stream.
     */
    public String getStream() {
        return stream;
    }

    /**
     * Sets the name of the stream
     * @param stream the name fo the stream.
     */
    public void setStream(String stream) {
        if (stream == null || stream.length() == 0 || stream.contains(">") ||
            stream.contains(".") || stream.contains("*")) {
            throw new IllegalArgumentException("stream cannot be null, empty, tokenized, or wildcarded");
        }
        this.stream = stream;
    }

    /**
     * Gets the publish timeout.
     * @return the publish timeout.
     */
    public Duration getStreamTimeout() {
        return streamTimeout;
    }

    /**
     * Sets the publish timeout.
     * @param timeout the publish timeout.
     */
    public void setStreamTimeout(Duration timeout) {
        this.streamTimeout = timeout;
    }

    /**
     * Creates a builder for the publish options.
     * @return the builder.s
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * PublishOptions are created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls. The builder can also
     * be created from a properties object using the property names defined with the
     * prefix PROP_ in this class.
     */
    public static class Builder {
        String stream = PublishOptions.unspecifiedStream;
        Duration streamTimeout = PublishOptions.defaultTimeout;

        /**
         * Constructs a new publish options Builder with the default values.
         */
        public Builder() {
            // NOOP.
        }
        
        /**
         * Constructs a builder from properties
         * @param properties properties
         */
        public Builder(Properties properties) {
            String s = properties.getProperty(PublishOptions.PROP_PUBLISH_TIMEOUT);
            if (s != null) {
                streamTimeout = Duration.parse(s);
            }

            s = properties.getProperty((PublishOptions.PROP_STREAM_NAME));
            if (s != null) {
                stream = s;
            }
        }

        /**
         * Sets the stream name for publishing.  The default is undefined.
         * @param stream The name of the stream.
         * @return Builder
         */
        public Builder stream(String stream) {
            this.stream = stream;
            return this;
        }

        /**
         * Sets the timeout to wait for a publish acknowledgement from a jetstream
         * enabled NATS server.
         * @param timeout the publish timeout.
         * @return Builder
         */
        public Builder streamTimeout(Duration timeout) {
            this.streamTimeout = timeout;
            return this;
        }

        /**
         * Builds the publish options.
         * @return publish options
         */
        public PublishOptions build() {
            return new PublishOptions(stream, streamTimeout);
        }
    }

}
