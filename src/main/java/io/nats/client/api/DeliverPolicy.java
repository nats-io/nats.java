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

import org.jspecify.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * The delivery policy for this consumer, the point in the stream from which to receive messages
 */
public enum DeliverPolicy {
    /** all messages */
    All("all"),

    /** start at the last message */
    Last("last"),

    /** start at any new messages */
    New("new"),

    /** by start sequence */
    ByStartSequence("by_start_sequence"),

    /** by start time */
    ByStartTime("by_start_time"),

    /** last per subject */
    LastPerSubject("last_per_subject");

    private final String policy;

    DeliverPolicy(String p) {
        policy = p;
    }

    @Override
    public String toString() {
        return policy;
    }

    private static final Map<String, DeliverPolicy> strEnumHash = new HashMap<>();

    static {
        for (DeliverPolicy env : DeliverPolicy.values()) {
            strEnumHash.put(env.toString(), env);
        }
    }

    /**
     * Get an instance from the JSON value
     * @param value the value
     * @return the instance or null if the string is not matched
     */
    @Nullable
    public static DeliverPolicy get(String value) {
        return strEnumHash.get(value);
    }
}
