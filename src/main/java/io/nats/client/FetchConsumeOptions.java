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

import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.support.JsonValue;

import static io.nats.client.support.ApiConstants.EXPIRES_IN;
import static io.nats.client.support.ApiConstants.NO_WAIT;
import static io.nats.client.support.JsonUtils.addFldWhenTrue;
import static io.nats.client.support.JsonValueUtils.readBoolean;
import static io.nats.client.support.JsonValueUtils.readLong;

/**
 * Fetch Consume Options are provided to customize the fetch operation.
 */
public class FetchConsumeOptions extends BaseConsumeOptions {
    public static final FetchConsumeOptions DEFAULT_FETCH_OPTIONS = FetchConsumeOptions.builder().build();

    private final boolean noWait;

    private FetchConsumeOptions(Builder b) {
        super(b);
        this.noWait = b.noWait;
    }

    @Override
    protected void subclassSpecificToJson(StringBuilder sb) {
        addFldWhenTrue(sb, NO_WAIT, noWait);
    }

    /**
     * The maximum number of messages to fetch.
     * @return the maximum number of messages to fetch
     */
    public int getMaxMessages() {
        return messages;
    }

    /**
     * The maximum number of bytes to fetch.
     * @return the maximum number of bytes to fetch
     */
    public long getMaxBytes() {
        return bytes;
    }

    public boolean isNoWait() {
        return noWait;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder
        extends BaseConsumeOptions.Builder<Builder, FetchConsumeOptions> {

        protected boolean noWait = false;

        protected Builder getThis() { return this; }

        @Override
        public Builder jsonValue(JsonValue jsonValue) {
            super.jsonValue(jsonValue);
            if (readBoolean(jsonValue, NO_WAIT, false)) {
                noWaitExpiresIn(readLong(jsonValue, EXPIRES_IN, ConsumerConfiguration.LONG_UNSET));
            }
            return this;
        }

        /**
         * Set the maximum number of messages to fetch and remove any previously set {@link #maxBytes(long)} constraint.
         * The number of messages fetched will also be constrained by the expiration time.
         * <p>Less than 1 means default of {@value BaseConsumeOptions#DEFAULT_MESSAGE_COUNT}.</p>
         * @param maxMessages the number of messages.
         * @return the builder
         */
        public Builder maxMessages(int maxMessages) {
            messages(maxMessages);
            return bytes(-1);
        }

        /**
         * Set maximum number of bytes to fetch and remove any previously set {@link #maxMessages constraint}
         * The number of bytes fetched will also be constrained by the expiration time.
         * <p>Less than 1 removes any previously set max bytes constraint.</p>
         * <p>It is important to set the byte size greater than your largest message payload, plus some amount
         * to account for overhead, otherwise the consume process will stall if there are no messages that fit the criteria.</p>
         * @see Message#consumeByteCount()
         * @param maxBytes the maximum bytes
         * @return the builder
         */
        public Builder maxBytes(long maxBytes) {
            return super.bytes(maxBytes);
        }

        /**
         * Set maximum number of bytes or messages to fetch.
         * The number of messages/bytes fetched will also be constrained by
         * whichever constraint is reached first, as well as the expiration time.
         * <p>Less than 1 max bytes removes any previously set max bytes constraint.</p>
         * <p>Less than 1 max messages removes any previously set max messages constraint.</p>
         * <p>It is important to set the byte size greater than your largest message payload, plus some amount
         * to account for overhead, otherwise the consume process will stall if there are no messages that fit the criteria.</p>
         * @see Message#consumeByteCount()
         * @param maxBytes the maximum bytes
         * @param maxMessages the maximum number of messages
         * @return the builder
         */
        public Builder max(int maxBytes, int maxMessages) {
            messages(maxMessages);
            return bytes(maxBytes);
        }

        @Override
        public Builder expiresIn(long expiresInMillis) {
            if (noWait && expiresInMillis < 1) {
                expiresIn = ConsumerConfiguration.LONG_UNSET;
                return this;
            }
            return super.expiresIn(expiresInMillis);
        }

        /**
         * Set no wait to true
         * When no wait is true, the fetch will return immediately with as many messages as are available. Between zero and the maximum configured.
         * @return the builder
         */
        public Builder noWait() {
            this.noWait = true;
            expiresIn = ConsumerConfiguration.LONG_UNSET;
            return this;
        }

        /**
         * Set no wait to true with an expiration. This is the common configuration to receive messages as soon as they arrive in the stream without excessive pulling.
         * When no wait is true with expire, the fetch will return immediately with as many messages as are available, but at least one message. Between one and the maximum configured.
         * When no message is available it will wait for new messages to arrive till it expires.
         * @param expiresInMillis the expiration time in milliseconds
         * @return the builder
         */
        public Builder noWaitExpiresIn(long expiresInMillis) {
            this.noWait = true;
            return expiresIn(expiresInMillis);
        }

        /**
         * Build the FetchConsumeOptions.
         * @return a FetchConsumeOptions instance
         */
        public FetchConsumeOptions build() {
            return new FetchConsumeOptions(this);
        }
    }
}
