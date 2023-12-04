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

import static io.nats.client.support.Validator.*;

/**
 * The PublishOptions class specifies the options for publishing with JetStream enabled servers.
 * Options are created using a {@link PublishOptions.Builder Builder}.
 */
public class PublishOptions {
    /**
     * Use this variable for timeout in publish options.
     */
    public static final Duration DEFAULT_TIMEOUT = Options.DEFAULT_CONNECTION_TIMEOUT;

    /**
     * Use this variable to unset a stream in publish options.
     */
    public static final String UNSET_STREAM = null;

    /**
     * Use this variable to unset a sequence number in publish options.
     */
    public static final long UNSET_LAST_SEQUENCE = -1;

    private final String stream;
    private final Duration streamTimeout;
    private final String expectedStream;
    private final String expectedLastId;
    private final long expectedLastSeq;
    private final long expectedLastSubSeq;
    private final String msgId;
    private final Serializer serializer;

    private PublishOptions(String stream, Duration streamTimeout, String expectedStream, String expectedLastId, long expectedLastSeq, long expectedLastSubSeq, String msgId, Serializer serializer) {
        this.stream = stream;
        this.streamTimeout = streamTimeout;
        this.expectedStream = expectedStream;
        this.expectedLastId = expectedLastId;
        this.expectedLastSeq = expectedLastSeq;
        this.expectedLastSubSeq = expectedLastSubSeq;
        this.msgId = msgId;
        this.serializer = serializer;
    }

    /**
     * Property used to configure a builder from a Properties object.
     */
    public static final String PROP_STREAM_NAME = Options.PFX + "publish.stream";

    /**
     * Property used to configure a builder from a Properties object..
     */
    public static final String PROP_PUBLISH_TIMEOUT = Options.PFX + "publish.timeout";

    /**
     * Gets the name of the stream.
     * @return the name of the stream.
     */
    public String getStream() {
        return stream;
    }

    /**
     * Gets the publish timeout.
     * @return the publish timeout.
     */
    public Duration getStreamTimeout() {
        return streamTimeout;
    }

    /**
     * Gets the expected stream.
     * @return the stream.
     */
    public String getExpectedStream() {
        return expectedStream;
    }

    /**
     * Gets the expected last message ID in the stream.
     * @return the message ID.
     */
    public String getExpectedLastMsgId() {
        return expectedLastId;
    }

    /**
     * Gets the expected last sequence number of the stream.
     * @return sequence number
     */
    public long getExpectedLastSequence() {
        return expectedLastSeq;
    }

    /**
     * Gets the expected last subject sequence number of the stream.
     * @return sequence number
     */
    public long getExpectedLastSubjectSequence() {
        return expectedLastSubSeq;
    }

    public Serializer getSerializer() { return this.serializer; }


    /**
     * Gets the message ID
     * @return the message id;
     */
    public String getMessageId() {
        return this.msgId;
    }

    /**
     * Creates a builder for the options.
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
        String stream = UNSET_STREAM;
        Duration streamTimeout = DEFAULT_TIMEOUT;
        String expectedStream;
        String expectedLastId;
        long expectedLastSeq = UNSET_LAST_SEQUENCE;
        long expectedLastSubSeq = UNSET_LAST_SEQUENCE;
        String msgId;
        Serializer serializer;

        /**
         * Constructs a new publish options Builder with the default values.
         */
        public Builder() {}

        /**
         * Constructs a builder from properties
         * @param properties properties
         */
        public Builder(Properties properties) {
            String s = properties.getProperty(PublishOptions.PROP_PUBLISH_TIMEOUT);
            if (s != null) {
                streamTimeout = Duration.parse(s);
            }

            s = properties.getProperty(PublishOptions.PROP_STREAM_NAME);
            if (s != null) {
                stream = s;
            }
        }

        /**
         * Sets the stream name for publishing. The default is undefined.
         * @param stream The name of the stream.
         * @return Builder
         */
        public Builder stream(String stream) {
            this.stream = validateStreamName(stream, false);
            return this;
        }

        /**
         * Sets the timeout to wait for a publish acknowledgement from a JetStream
         * enabled NATS server.
         * @param timeout the publish timeout.
         * @return Builder
         */
        public Builder streamTimeout(Duration timeout) {
            this.streamTimeout = validateDurationNotRequiredGtOrEqZero(timeout, DEFAULT_TIMEOUT);
            return this;
        }

        /**
         * Sets the expected stream for the publish. If the
         * stream does not match the server will not save the message.
         * @param stream expected stream
         * @return builder
         */
        public Builder expectedStream(String stream) {
            expectedStream = validateStreamName(stream, false);
            return this;
        }

        /**
         * Sets the expected last ID of the previously published message.  If the
         * message ID does not match the server will not save the message.
         * @param lastMsgId the stream
         * @return builder
         */
        public Builder expectedLastMsgId(String lastMsgId) {
            expectedLastId = emptyAsNull(lastMsgId);
            return this;
        }

        /**
         * Sets the expected message sequence of the publish
         * @param sequence the expected last sequence number
         * @return builder
         */
        public Builder expectedLastSequence(long sequence) {
            // 0 has NO meaning to expectedLastSequence but we except 0 b/c the sequence is really a ulong
            expectedLastSeq = validateGtEqMinus1(sequence, "Last Sequence");
            return this;
        }

        /**
         * Sets the expected subject message sequence of the publish
         * @param sequence the expected last subject sequence number
         * @return builder
         */
        public Builder expectedLastSubjectSequence(long sequence) {
            expectedLastSubSeq = validateGtEqMinus1(sequence, "Last Subject Sequence");
            return this;
        }

        /**
         * Sets the message id. Message IDs are used for de-duplication
         * and should be unique to each message payload.
         * @param msgId the unique message id.
         * @return builder
         */
        public Builder messageId(String msgId) {
            this.msgId = emptyAsNull(msgId);
            return this;
        }

        /**
         * Clears the expected so the build can be re-used.
         * Clears the expectedLastId, expectedLastSequence and messageId fields.
         * @return builder
         */
        public Builder clearExpected() {
            expectedLastId = null;
            expectedLastSeq = UNSET_LAST_SEQUENCE;
            expectedLastSubSeq = UNSET_LAST_SEQUENCE;
            msgId = null;
            return this;
        }

        public Builder serializer(Serializer serializer) {
            this.serializer = serializer;
            return this;
        }

        /**
         * Builds the publish options.
         * @return publish options
         */
        public PublishOptions build() {
            return new PublishOptions(stream, streamTimeout, expectedStream, expectedLastId, expectedLastSeq, expectedLastSubSeq, msgId, serializer);
        }
    }
}
