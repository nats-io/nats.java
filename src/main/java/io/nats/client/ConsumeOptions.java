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
         * Set the initial batch message size.
         * @param batchSize the batch size. Must be greater than 0
         * @return the builder
         */
        public Builder batchSize(int batchSize) {
            return super.messages(batchSize);
        }

        /**
         * Set the initial batch byte size. When set (a value greater than zero,)
         * it is used in conjunction with batch size, meaning whichever limit is reached
         * first is respected.
         * @param bytes the batch bytes
         * @param messages the number of messages. Must be greater than 0.
         * @return the builder
         */
        public Builder batchBytes(int bytes, int messages) {
            return super.bytes(bytes, messages);
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
