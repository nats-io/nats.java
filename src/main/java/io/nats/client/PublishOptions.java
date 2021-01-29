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

import static io.nats.client.support.Validator.nullOrEmpty;
import static io.nats.client.support.Validator.validateStreamName;

/**
 * The PublishOptions class specifies the options for publishing with JetStream enabled servers.
 * Options are created using a {@link PublishOptions.Builder Builder}.
 */
public class PublishOptions {
    /**
     * Use this variable for timeout in publish options.
     */
    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(2);

    /**
     * Use this variable to unset a stream in publish options.
     */
    public static final String UNSET_STREAM = null;

    /**
     * Use this variable to unset a sequence number in publish options.
     */
    public static final long UNSET_LAST_SEQUENCE = -1;

    private String stream = UNSET_STREAM;
    private Duration streamTimeout = DEFAULT_TIMEOUT;
    private String expectedStream;
    private String expectedLastId;
    private long expectedLastSeq = UNSET_LAST_SEQUENCE;
    private String msgId;

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
     * Sets the name of the stream
     * @param stream the name of the stream.
     */
    public void setStream(String stream) {
        this.stream = nullOrEmpty(stream) ? UNSET_STREAM : validateStreamName(stream);
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
     * Sets the expected stream of the publish. If the
     * stream does not match the server will not save the message.
     * @param stream expected stream
     */
    public void setExpectedStream(String stream) {
        expectedStream = stream;
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
     * Sets the expected last ID of the previously published message.  If the
     * message ID does not match the server will not save the message.
     * @param lastMsgId the expected last message ID in the stream.
     */
    public void setExpectedLastMsgId(String lastMsgId) {
        expectedLastId = lastMsgId;
    }

    /**
     * Gets the expected last sequence number of the stream.
     * @return sequence number
     */
    public long getExpectedLastSequence() {
        return expectedLastSeq;
    }

    /**
     * Sets the expected last sequence of the previously published message.  If the
     * sequence does not match the server will not save the message.
     * @param sequence the expected last sequence number of the stream.
     */
    public void setExpectedLastSeqence(long sequence) {
        expectedLastSeq = sequence;
    }

    /**
     * Gets the message ID
     * @return the message id;
     */
    public String getMessageId() {
        return this.msgId;
    }

    /**
     * Sets the message id. Message IDs are used for de-duplication
     * and should be unique to each message payload.
     * @param msgId the unique message id.
     */
    public void setMessageId(String msgId) {
        this.msgId = msgId;
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
        String stream = UNSET_STREAM;
        Duration streamTimeout = DEFAULT_TIMEOUT;
        String expectedStream;
        String expectedLastId;
        long expectedLastSeq;
        String msgId;

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
         * Sets the expected stream of the publish. If the 
         * stream does not match the server will not save the message.
         * @param stream expected stream
         * @return builder
         */
        public Builder expectedStream(String stream) {
            expectedStream = stream;
            return this;
        }

        /**
         * Sets the expected last ID of the previously published message.  If the 
         * message ID does not match the server will not save the message.
         * @param lastMsgId the stream
         * @return builder
         */
        public Builder expectedLastMsgId(String lastMsgId) {
            expectedLastId = lastMsgId;
            return this;
        }        

        /**
         * Sets the expected message ID of the publish
         * @param sequence the expected last sequence number
         * @return builder
         */
        public Builder expectedLastSeqence(long sequence) {
            expectedLastSeq = sequence;
            return this;
        }

        /**
         * Sets the message id. Message IDs are used for de-duplication
         * and should be unique to each message payload.
         * @param msgId the unique message id.
         */
        public void messageId(String msgId) {
            this.msgId = msgId;
        }

        /**
         * Builds the publish options.
         * @return publish options
         */
        public PublishOptions build() {
            PublishOptions po = new PublishOptions();
            po.setStream(stream);
            po.setStreamTimeout(streamTimeout);
            po.setExpectedLastMsgId(expectedLastId);
            po.setExpectedLastSeqence(expectedLastSeq);
            po.setExpectedStream(expectedStream);
            po.setMessageId(msgId);
            return po;
        }
    }
}

