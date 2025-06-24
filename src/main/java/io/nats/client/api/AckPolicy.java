// Copyright 2020 The NATS Authors
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

import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents the Ack Policy of a consumer
 */
public enum AckPolicy {
	/**
     * Messages are acknowledged as soon as the server sends them. Clients do not need to ack.
     */
    None("none"),
    /**
     * All messages with a sequence number less than the message acked are also acknowledged. E.g. reading a batch of messages 1 .. 100. Ack on message 100 will acknowledge 1 .. 99 as well.
     */
    All("all"),
    /**
     * Each message must be acknowledged individually. Message can be acked out of sequence and create gaps of unacknowledged messages in the consumer.
     */
    Explicit("explicit");

    private final String policy;

    AckPolicy(String p) {
        policy = p;
    }

    @Override
    public String toString() {
        return policy;
    }

    private static final Map<String, AckPolicy> strEnumHash = new HashMap<>();

    static {
        for (AckPolicy env : AckPolicy.values()) {
            strEnumHash.put(env.toString(), env);
        }
    }

    @Nullable
    public static AckPolicy get(String value) {
        return strEnumHash.get(value);
    }
}
