// Copyright 2023 The NATS Authors
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

import io.nats.client.support.JsonParseException;
import io.nats.client.support.JsonParser;
import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonValue;
import org.jspecify.annotations.NonNull;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.*;
import static io.nats.client.support.JsonValueUtils.*;
import static io.nats.client.support.JsonValueUtils.readBoolean;
import static io.nats.client.support.JsonValueUtils.readInteger;
import static io.nats.client.support.JsonValueUtils.readLong;

/**
 * Base Consume Options are provided to customize the way the consume and
 * fetch operate. It is the base class for ConsumeOptions and FetchConsumeOptions.
 */
public class BaseConsumeOptions implements JsonSerializable {
    /** constant for default message count */
    public static final int DEFAULT_MESSAGE_COUNT = 500;
    /** constant for default message count when bytes */
    public static final int DEFAULT_MESSAGE_COUNT_WHEN_BYTES = 1_000_000;
    /** constant for default threshold percent */
    public static final int DEFAULT_THRESHOLD_PERCENT = 25;
    /** constant for default expires in millis */
    public static final long DEFAULT_EXPIRES_IN_MILLIS = 30000;
    /** constant for min expires mills */
    public static final long MIN_EXPIRES_MILLS = 1000;
    /** constant for max heartbeat millis */
    public static final long MAX_HEARTBEAT_MILLIS = 30000;
    /** constant for max idle heartbeat percent */
    public static final int MAX_IDLE_HEARTBEAT_PERCENT = 50;

    protected final int messages;
    protected final long bytes;
    protected final long expiresIn;
    protected final int thresholdPercent;
    protected final long idleHeartbeat;
    protected final String group;
    protected final int priority;
    protected final long minPending;
    protected final long minAckPending;
    protected final boolean raiseStatusWarnings;

    protected BaseConsumeOptions(Builder<?, ?> b) {
        // Message / bytes is part of base and is calculated
        bytes = b.bytes;
        if (bytes > 0) {
            messages = b.messages < 0 ? DEFAULT_MESSAGE_COUNT_WHEN_BYTES : b.messages;
        }
        else {
            messages = b.messages < 0 ? DEFAULT_MESSAGE_COUNT : b.messages;
        }

        // Validation for expiresIn, if any extra, is handled in subclass builder
        expiresIn = b.expiresIn;
        thresholdPercent = b.thresholdPercent;

        // 3. idleHeartbeat is part of base and is calculated.
        idleHeartbeat = Math.min(MAX_HEARTBEAT_MILLIS, expiresIn * MAX_IDLE_HEARTBEAT_PERCENT / 100);

        this.group = b.group;
        this.priority = b.priority;
        this.minPending = b.minPending;
        this.minAckPending = b.minAckPending;
        raiseStatusWarnings = b.raiseStatusWarnings;
    }

    @Override
    @NonNull
    public String toJson() {
        StringBuilder sb = beginJson();
        addField(sb, MESSAGES, messages);
        addField(sb, BYTES, bytes);
        addField(sb, EXPIRES_IN, expiresIn);
        addField(sb, IDLE_HEARTBEAT, idleHeartbeat);
        addField(sb, THRESHOLD_PERCENT, thresholdPercent);
        addField(sb, GROUP, group);
        addField(sb, PRIORITY, priority);
        addField(sb, MIN_PENDING, minPending);
        addField(sb, MIN_ACK_PENDING, minAckPending);
        addFldWhenTrue(sb, RAISE_STATUS_WARNINGS, raiseStatusWarnings);
        subclassSpecificToJson(sb);
        return endJson(sb).toString();
    }

    protected void subclassSpecificToJson(StringBuilder sb) {}

    /**
     * Get the expires setting
     * @return the expires, in milliseconds
     */
    public long getExpiresInMillis() {
        return expiresIn;
    }

    /**
     * Get the idle heartbeat value
     * @return the idle heartbeat in milliseconds
     */
    public long getIdleHeartbeat() {
        return idleHeartbeat;
    }

    /**
     * Get the threshold percent setting
     * @return the threshold percent
     */
    public int getThresholdPercent() {
        return thresholdPercent;
    }

    /**
     * Whether to raise status warnings to the error listener
     * @return true if should raise status warnings
     */
    public boolean raiseStatusWarnings() {
        return raiseStatusWarnings;
    }

    /**
     * Get the priority group setting
     * A group must be set if a  {@link io.nats.client.api.PriorityPolicy} has been specified.
     * The group must be consistent with the groups specified in the ConsumerConfiguration.
     * @return the priority group
     */
    public String getGroup() {
        return group;
    }

    /**
     * Get the priority setting
     * @return the priority
     */
    public int getPriority() {
        return priority;
    }

    /**
     * Get the min pending setting as per the overflow priority group. See {@link io.nats.client.api.PriorityPolicy}.
     *
     * @return the min pending
     */
    public long getMinPending() {
        return minPending;
    }

    /**
     * Get the min ack pending setting as per the overflow priority group. See {@link io.nats.client.api.PriorityPolicy}.
     * @return the min ack pending
     */
    public long getMinAckPending() {
        return minAckPending;
    }

    protected static abstract class Builder<B, CO> {
        protected int messages = -1;
        protected long bytes = 0;
        protected int thresholdPercent = DEFAULT_THRESHOLD_PERCENT;
        protected long expiresIn = DEFAULT_EXPIRES_IN_MILLIS;
        protected boolean raiseStatusWarnings = false;
        protected String group;
        protected int priority;
        protected long minPending = -1;
        protected long minAckPending = -1;

        protected abstract B getThis();

        /**
         * Initialize values from the json string.
         * @param json the json string to parse
         * @return the builder
         * @throws JsonParseException if there is a problem parsing the json
         */
        public B json(String json) throws JsonParseException {
            return jsonValue(JsonParser.parse(json));
        }

        /**
         * Initialize values from the JsonValue object.
         * @param jsonValue the json value object
         * @return the builder
         */
        public B jsonValue(JsonValue jsonValue) {
            messages(readInteger(jsonValue, MESSAGES, -1));
            bytes(readLong(jsonValue, BYTES, -1));
            expiresIn(readLong(jsonValue, EXPIRES_IN, DEFAULT_EXPIRES_IN_MILLIS));
            thresholdPercent(readInteger(jsonValue, THRESHOLD_PERCENT, -1));
            raiseStatusWarnings(readBoolean(jsonValue, RAISE_STATUS_WARNINGS, false));
            group(readStringEmptyAsNull(jsonValue, GROUP));
            priority(readInteger(jsonValue, PRIORITY, 0));
            minPending(readLong(jsonValue, MIN_PENDING, -1));
            minAckPending(readLong(jsonValue, MIN_ACK_PENDING, -1));
            return getThis();
        }

        protected B messages(int messages) {
            this.messages = messages < 1 ? -1 : messages;
            return getThis();
        }

        protected B bytes(long bytes) {
            this.bytes = bytes < 1 ? 0 : bytes;
            return getThis();
        }

        /**
         * In Fetch, sets the maximum amount of time to wait to reach the batch size or max byte.
         * In Consume, sets the maximum amount of time for an individual pull to be open
         * before issuing a replacement pull.
         * <p>Zero or less will default to {@value BaseConsumeOptions#DEFAULT_EXPIRES_IN_MILLIS},
         * otherwise, cannot be less than {@value BaseConsumeOptions#MIN_EXPIRES_MILLS}</p>
         * @param expiresInMillis the expiration time in milliseconds
         * @return the builder
         */
        public B expiresIn(long expiresInMillis) {
            if (expiresInMillis < 1) {
                expiresIn = DEFAULT_EXPIRES_IN_MILLIS;
            }
            else if (expiresInMillis < MIN_EXPIRES_MILLS) {
                throw new IllegalArgumentException("Expires must be greater than or equal to " + MIN_EXPIRES_MILLS);
            }
            else {
                expiresIn = expiresInMillis;
            }
            return getThis();
        }

        /**
         * Set the threshold percent of max bytes (if max bytes is specified) or messages
         * that will trigger issuing pull requests to keep messages flowing.
         * <p>Only applies to endless consumes.</p>
         * <p>For instance if the batch size is 100 and the re-pull percent is 25,
         * the first pull will be for 100, and then when 25 messages have been received
         * another 75 will be requested, keeping the number of messages in transit always at 100.</p>
         * <p>Must be between 1 and 100 inclusive.
         * Less than 1 will assume the default of {@value BaseConsumeOptions#DEFAULT_THRESHOLD_PERCENT}.
         * Greater than 100 will assume 100. </p>
         * @param thresholdPercent the threshold percent
         * @return the builder
         */
        public B thresholdPercent(int thresholdPercent) {
            this.thresholdPercent = thresholdPercent < 1 ? DEFAULT_THRESHOLD_PERCENT : Math.min(100, thresholdPercent);
            return getThis();
        }

        /**
         * Raise status warning turns on sending status messages to the error listener.
         * The default is to not raise status warnings
         * @return the builder
         */
        public B raiseStatusWarnings() {
            this.raiseStatusWarnings = true;
            return getThis();
        }

        /**
         * Turn on or off raise status warning turns. When on, status messages are sent to the error listener.
         * The default is to not raise status warnings
         * @param raiseStatusWarnings flag indicating whether to raise status messages
         * @return the builder
         */
        public B raiseStatusWarnings(boolean raiseStatusWarnings) {
            this.raiseStatusWarnings = raiseStatusWarnings;
            return getThis();
        }

        /**
         * Sets the group
         * @param group the priority group for this pull
         * @return Builder
         */
        public B group(String group) {
            this.group = group;
            return getThis();
        }

        /**
         * Sets the priority for the group
         * @param priority the priority
         * @return Builder
         */
        public B priority(int priority) {
            this.priority = priority;
            return getThis();
        }

        /**
         * When specified, the consumer will only receive messages when the consumer has at least this many pending messages.
         * @param minPending the min pending
         * @return the builder
         */
        public B minPending(long minPending) {
            this.minPending = minPending < 1 ? -1 : minPending;
            return getThis();
        }

        /**
         * When specified, the consumer will only receive messages when the consumer has at least this many ack pending messages.
         * @param minAckPending the min ack pending
         * @return the builder
         */
        public B minAckPending(long minAckPending) {
            this.minAckPending = minAckPending < 1 ? -1 : minAckPending;
            return getThis();
        }

        /**
         * Build the options.
         * @return the built options
         */
        public abstract CO build();
    }
}
