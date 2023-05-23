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
 * Consume Options are provided to customize the consume operation.
 * SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
 */
public class ConsumeOptions extends BaseConsumeOptions {
    public static ConsumeOptions DEFAULT_CONSUME_OPTIONS = ConsumeOptions.builder().build();

    private ConsumeOptions(Builder b) {
        super(b);
    }

    /**
     * The initial batch message size.
     * @return the initial batch message size
     */
    public int getBatchSize() {
        return messages;
    }
    /**
     * The initial batch byte size.
     * @return the initial batch byte size
     */
    public int getBatchBytes() {
        return bytes;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder
        extends BaseConsumeOptions.Builder<Builder, ConsumeOptions> {

        protected Builder getThis() { return this; }

        /**
         * Set the initial batch size in messages.
         * @param batchSize the batch size. Must be greater than 0
         *                  or will default to {@value BaseConsumeOptions#DEFAULT_MESSAGE_COUNT_WHEN_BYTES}
         * @return the builder
         */
        public Builder batchSize(int batchSize) {
            return super.messagesAndBytes(batchSize, -1);
        }

        /**
         * Set the initial batch size in bytes.
         * @param batchBytes the batch bytes
         * @return the builder
         */
        public Builder batchBytes(int batchBytes) {
            return super.messagesAndBytes(-1, batchBytes);
        }

        /**
         * Build the ConsumeOptions.
         * @return the built ConsumeOptions
         */
        public ConsumeOptions build() {
            return new ConsumeOptions(this);
        }
    }
}
