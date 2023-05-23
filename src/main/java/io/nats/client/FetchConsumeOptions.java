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

/**
 * Fetch Consume Options are provided to customize the fetch operation.
 * SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
 */
public class FetchConsumeOptions extends BaseConsumeOptions {
    public static FetchConsumeOptions DEFAULT_FETCH_OPTIONS = FetchConsumeOptions.builder().build();

    private FetchConsumeOptions(Builder b) {
        super(b);
    }

    /**
     * The maximum number of messages to fetch
     * @return the maximum number of messages to fetch
     */
    public int getMaxMessages() {
        return messages;
    }

    /**
     * The maximum number of bytes to fetch
     * @return the maximum number of bytes to fetch
     */
    public int getMaxBytes() {
        return bytes;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder
        extends BaseConsumeOptions.Builder<Builder, FetchConsumeOptions> {

        protected Builder getThis() { return this; }

        /**
         * Set the maximum number of messages to fetch.
         * @param messages the number of messages. Must be greater than 0
         * @return the builder
         */
        public Builder maxMessages(int messages) {
            return super.messages(messages);
        }

        /**
         * The maximum bytes to consume for Fetch. When set (a value greater than zero,)
         * it is used in conjunction with max messages, meaning whichever limit is reached
         * first is respected.
         * @param bytes the maximum bytes
         * @return the builder
         */
        public Builder maxBytes(int bytes) {
            if (this.messages == -1) {
                return super.bytes(bytes, DEFAULT_MESSAGE_COUNT_WHEN_BYTES);
            }
            return super.bytes(bytes, messages);
        }

        /**
         * The maximum bytes to consume for Fetch. When set (a value greater than zero,)
         * it is used in conjunction with max messages, meaning whichever limit is reached
         * first is respected.
         * @param bytes the maximum bytes
         * @param messages the number of messages. Must be greater than 0.
         * @return the builder
         */
        public Builder maxBytes(int bytes, int messages) {
            return super.bytes(bytes, messages);
        }

        /**
         * Build the FetchConsumeOptions.
         * @return the built FetchConsumeOptions
         */
        public FetchConsumeOptions build() {
            return new FetchConsumeOptions(this);
        }
    }
}
