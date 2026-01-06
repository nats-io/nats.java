// Copyright 2015-2025 The NATS Authors
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
     * @deprecated Just use null to unset
     * Use this variable to unset a stream in publish options.
     */
    @Deprecated
    public static final String UNSET_STREAM = null;

    /**
     * Use this variable to unset a sequence number in publish options.
     */
    public static final long UNSET_LAST_SEQUENCE = -1;

    private final String pubAckStream;
    private final Duration streamTimeout;
    private final String expectedStream;
    private final String expectedLastMsgId;
    private final long expectedLastSeq;
    private final long expectedLastSubSeq;
    private final String expectedLastSubSeqSubject;
    private final String msgId;
    private final MessageTtl messageTtl;

    private PublishOptions(Builder b) {
        this.pubAckStream = b.pubAckStream;
        this.streamTimeout = b.streamTimeout;
        this.expectedStream = b.expectedStream;
        this.expectedLastMsgId = b.expectedLastMsgId;
        this.expectedLastSeq = b.expectedLastSeq;
        this.expectedLastSubSeq = b.expectedLastSubSeq;
        this.expectedLastSubSeqSubject = b.expectedLastSubSeqSubject;
        this.msgId = b.msgId;
        this.messageTtl = b.messageTtl;
    }

    @Override
    public String toString() {
        return "PublishOptions{" +
            "pubAckStream='" + pubAckStream + '\'' +
            ", streamTimeout=" + streamTimeout +
            ", expectedStream='" + expectedStream + '\'' +
            ", expectedLastMsgId='" + expectedLastMsgId + '\'' +
            ", expectedLastSeq=" + expectedLastSeq +
            ", expectedLastSubSeq=" + expectedLastSubSeq +
            ", expectedLastSubSeqSub=" + expectedLastSubSeqSubject +
            ", msgId='" + msgId + '\'' +
            ", messageTtl=" + getMessageTtl() +
            '}';
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
     * @deprecated this field isn't really very useful since it's used after the publish
     * Gets the name of the stream to check after the publish has succeeded
     * @return the name of the stream.
     */
    @Deprecated
    public String getStream() {
        return pubAckStream;
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
        return expectedLastMsgId;
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
     * @return last subject sequence number
     */
    public long getExpectedLastSubjectSequence() {
        return expectedLastSubSeq;
    }

    /**
     * Gets the expected subject to limit last subject sequence number of the stream.
     * @return the last subject sequence number's limit subject
     */
    public String getExpectedLastSubjectSequenceSubject() {
        return expectedLastSubSeqSubject;
    }

    /**
     * Gets the message ID
     * @return the message id;
     */
    public String getMessageId() {
        return this.msgId;
    }

    /**
     * Gets the message ttl string. Might be null. Might be "never".
     * 10 seconds would be "10s" for the server
     * @return the message ttl string
     */
    public String getMessageTtl() {
        return messageTtl == null ? null : messageTtl.getTtlString();
    }

    /**
     * Creates a builder for the options.
     * @return the builder
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
        String pubAckStream = null;
        Duration streamTimeout = DEFAULT_TIMEOUT;
        String expectedStream;
        String expectedLastMsgId;
        long expectedLastSeq = UNSET_LAST_SEQUENCE;
        long expectedLastSubSeq = UNSET_LAST_SEQUENCE;
        String expectedLastSubSeqSubject;
        String msgId;
        MessageTtl messageTtl;

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
                pubAckStream = s;
            }
        }

        /**
         * @deprecated Not a very useful function
         * Sets the stream name to expect the pub ack to have.
         * This really should never be an issue and it does
         * not prevent the message from being published,
         * it's an exception after the fact
         * @param stream The name of the stream.
         * @return The Builder
         */
        @Deprecated
        public Builder stream(String stream) {
            this.pubAckStream = validateStreamName(stream, false);
            return this;
        }

        /**
         * Sets the timeout to wait for a publish acknowledgement from a JetStream
         * enabled NATS server.
         * @param timeout the publish timeout.
         * @return The Builder
         */
        public Builder streamTimeout(Duration timeout) {
            this.streamTimeout = validateDurationNotRequiredGtOrEqZero(timeout, DEFAULT_TIMEOUT);
            return this;
        }

        /**
         * Sets the expected stream for the publish. If the
         * stream does not match the server will not save the message.
         * @param stream expected stream
         * @return The Builder
         */
        public Builder expectedStream(String stream) {
            expectedStream = validateStreamName(stream, false);
            return this;
        }

        /**
         * Sets the expected last ID of the previously published message.  If the
         * message ID does not match the server will not save the message.
         * @param lastMsgId the stream
         * @return The Builder
         */
        public Builder expectedLastMsgId(String lastMsgId) {
            expectedLastMsgId = emptyAsNull(lastMsgId);
            return this;
        }

        /**
         * Sets the expected message sequence of the publish
         * @param sequence the expected last sequence number
         * @return The Builder
         */
        public Builder expectedLastSequence(long sequence) {
            // 0 has NO meaning to expectedLastSequence but we except 0 b/c the sequence is really a ulong
            expectedLastSeq = validateGtEqMinus1(sequence, "Last Sequence");
            return this;
        }

        /**
         * Sets the expected subject message sequence of the publish
         * @param sequence the expected last subject sequence number
         * @return The Builder
         */
        public Builder expectedLastSubjectSequence(long sequence) {
            expectedLastSubSeq = validateGtEqMinus1(sequence, "Last Subject Sequence");
            return this;
        }

        /**
         * Sets the filter subject for the expected last subject sequence
         * This can be used for a wildcard since it is used
         * in place of the message subject along with expectedLastSubjectSequence
         * @param expectedLastSubSeqSubject the filter subject
         * @return The Builder
         */
        public Builder expectedLastSubjectSequenceSubject(String expectedLastSubSeqSubject) {
            this.expectedLastSubSeqSubject = expectedLastSubSeqSubject;
            return this;
        }

        /**
         * Sets the message id. Message IDs are used for de-duplication
         * and should be unique to each message payload.
         * @param msgId the unique message id.
         * @return The Builder
         */
        public Builder messageId(String msgId) {
            this.msgId = emptyAsNull(msgId);
            return this;
        }

        /**
         * Sets the TTL for this specific message to be published.
         * Less than 1 has the effect of clearing the message ttl
         * @param msgTtlSeconds the ttl in seconds
         * @return The Builder
         */
        public Builder messageTtlSeconds(int msgTtlSeconds) {
            this.messageTtl = msgTtlSeconds < 1 ? null : MessageTtl.seconds(msgTtlSeconds);
            return this;
        }

        /**
         * Sets the TTL for this specific message to be published. Use at your own risk.
         * The current specification can be found here @see <a href="https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-43.md#per-message-ttl">JetStream Per-Message TTL</a>
         * Null or empty has the effect of clearing the message ttl
         * @param msgTtlCustom the custom ttl string
         * @return The Builder
         */
        public Builder messageTtlCustom(String msgTtlCustom) {
            this.messageTtl = nullOrEmpty(msgTtlCustom) ? null : MessageTtl.custom(msgTtlCustom);
            return this;
        }

        /**
         * Sets the TTL for this specific message to be published and never be expired
         * @return The Builder
         */
        public Builder messageTtlNever() {
            this.messageTtl = MessageTtl.never();
            return this;
        }

        /**
         * Sets the TTL for this specific message to be published
         * @param messageTtl the message ttl instance
         * @return The Builder
         */
        public Builder messageTtl(MessageTtl messageTtl) {
            this.messageTtl = messageTtl;
            return this;
        }

        /**
         * Clears the expected so the build can be re-used.
         * Clears the following fields:
         * <ul>
         *   <li>expectedLastId</li>
         *   <li>expectedLastSequence</li>
         *   <li>expectedLastSubSeq</li>
         *   <li>expectedLastSubSeqSubject</li>
         *   <li>messageId</li>
         * </ul>
         * Does not clear the following fields:
         * <ul>
         *   <li>stream</li>
         *   <li>expectedStream</li>
         *   <li>streamTimeout</li>
         *   <li>messageTtl</li>
         * </ul>
         * @return The Builder
         */
        public Builder clearExpected() {
            expectedLastMsgId = null;
            expectedLastSeq = UNSET_LAST_SEQUENCE;
            expectedLastSubSeq = UNSET_LAST_SEQUENCE;
            expectedLastSubSeqSubject = null;
            msgId = null;

            return this;
        }

        /**
         * Builds the publish options.
         * @return publish options
         */
        public PublishOptions build() {
            return new PublishOptions(this);
        }
    }
}
