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
    private Duration timeout = defaultTimeout;

    /**
     * Property used to configure a builder from a Properties object. {@value #PROP_CONNECTION_CB}, see
     * {@link Builder#connectionListener(ConnectionListener) connectionListener}.
     */
    public static final String PROP_STREAM_NAME = Options.PFX + "publish.stream";

    /**
     * Property used to configure a builder from a Properties object. {@value #PROP_CONNECTION_CB}, see
     * {@link Builder#connectionListener(ConnectionListener) connectionListener}.
     */
    public static final String PROP_PUBLISH_TIMEOUT = Options.PFX + "publish.timeout";       

    public PublishOptions(String stream, Duration timeout) {
        this.stream = stream;
        this.timeout = timeout;
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
        if (stream == null || stream.length() == 0) {
            throw new IllegalArgumentException("stream cannot be null or empty");
        }
        this.stream = stream;
    }

    /**
     * Gets the publish timeout.
     * @return the publish timeout.
     */
    public Duration getTimeout() {
        return timeout;
    }

    /**
     * Sets the publish timeout.
     */
    public void setTimeout(Duration timeout) {
        this.timeout = timeout;
    }

    /**
     * Creates a builder for the publish options.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * 
     */
    public static class Builder {
        String stream = PublishOptions.unspecifiedStream;
        Duration timeout = PublishOptions.defaultTimeout;

        /**
         * Constructs a new publish options Builder with the default values.
         */
        public Builder() {
            // NOOP.
        }
        
        public Builder(Properties properties) {
            String s = properties.getProperty(PublishOptions.PROP_PUBLISH_TIMEOUT);
            if (s != null) {
                timeout = Duration.parse(s);
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
         * Sets the timeout to wait for a publish acknoledgement from a jetstream
         * enabled NATS server.
         * @param timeout the publish timeout.
         * @return
         */
        public Builder timeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        /**
         * Builds the publish options.
         * @return publish options
         */
        public PublishOptions build() {
            return new PublishOptions(stream, timeout);
        }
    }

}
