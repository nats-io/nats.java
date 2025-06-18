// Copyright 2022 The NATS Authors
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

package io.nats.client.api;

import io.nats.client.support.JsonSerializable;
import org.jetbrains.annotations.NotNull;

import static io.nats.client.support.ApiConstants.NO_ERASE;
import static io.nats.client.support.ApiConstants.SEQ;
import static io.nats.client.support.JsonUtils.*;

/**
 * Object used to make a request for message delete requests.
 */
public class MessageDeleteRequest implements JsonSerializable {
    private final long sequence;
    private final boolean erase;

    public MessageDeleteRequest(long sequence, boolean erase) {
        this.sequence = sequence;
        this.erase = erase;
    }

    public long getSequence() {
        return sequence;
    }

    public boolean isErase() {
        return erase;
    }

    public boolean isNoErase() {
        return !erase;
    }

    @Override
    @NotNull
    public String toJson() {
        StringBuilder sb = beginJson();
        addField(sb, SEQ, sequence);
        addFldWhenTrue(sb, NO_ERASE, isNoErase());
        return endJson(sb).toString();
    }

    /**
     * Creates a builder for the purge options
     * @return a purge options builder
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private long seq = -1;
        private boolean erase = true;

        /**
         * Set upper-bound sequence for messages to be deleted
         * @param seq the upper-bound sequence
         * @return the builder
         */
        public Builder sequence(final long seq) {
            this.seq = seq;
            return this;
        }

        /**
         * set to the default, erase the message on delete
         * @return the builder
         */
        public Builder erase() {
            this.erase = true;
            return this;
        }

        /**
         * set to not erase the message on delete
         * @return the builder
         */
        public Builder noErase() {
            this.erase = false;
            return this;
        }

        /**
         * Build the MessageDeleteRequest
         * @return the built MessageDeleteRequest
         */
        public MessageDeleteRequest build() {
            return new MessageDeleteRequest(seq, erase);
        }
    }
}
