// Copyright 2025 The NATS Authors
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

import io.nats.client.support.Validator;

/**
 * Class to make setting a per message ttl easier.
 */
public class MessageTtl {
    private final String ttlString;

    private MessageTtl(String ttlString) {
        this.ttlString = ttlString;
    }

    /**
     * Get the string representation of the message ttl, used as a value
     * @return the string
     */
    public String getTtlString() {
        return ttlString;
    }

    @Override
    public String toString() {
        return "MessageTtl{'" + ttlString + "'}";
    }

    /**
     * Sets the TTL for this specific message to be published
     * @param msgTtlSeconds the ttl in seconds
     * @return The MessageTtl instance
     */
    public static MessageTtl seconds(int msgTtlSeconds) {
        if (msgTtlSeconds < 1) {
            throw new IllegalArgumentException("Must be at least 1 second.");
        }
        return new MessageTtl(msgTtlSeconds + "s");
    }

    /**
     * Sets the TTL for this specific message to be published. Use at your own risk.
     * The current specification can be found here @see <a href="https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-43.md#per-message-ttl">JetStream Per-Message TTL</a>
     * @param messageTtlCustom the ttl in seconds
     * @return The MessageTtl instance
     */
    public static MessageTtl custom(String messageTtlCustom) {
        if (Validator.nullOrEmpty(messageTtlCustom)) {
            throw new IllegalArgumentException("Custom value required.");
        }
        return new MessageTtl(messageTtlCustom);
    }

    /**
     * Sets the TTL for this specific message to be published and never be expired
     * @return The MessageTtl instance
     */
    public static MessageTtl never() {
        return new MessageTtl("never");
    }
}
