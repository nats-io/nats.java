// Copyright 2024 The NATS Authors
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
 * Represents the Priority Policy of a consumer
 * @see <a href="https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-42.md">nats-io ADR-42</a>
 * Setting a priority policy will also require setting a Priority group. <BR>
 * When a priority policy and priority group are set, client instances making pull request need to specify the priority group
 * and optionally other ConsumerOptions See {@link io.nats.client.BaseConsumeOptions}
 *
 */
public enum PriorityPolicy {
    /** Standard consumer. All client instances are load balance fairly. */
    None("none"),
    /**
	 * Each client pull request specifies overflow limits in the  {@link io.nats.client.BaseConsumeOptions} <BR>
	 * Currently minPending and minAckPending are respected. <BR>
	 * When either the minimum number of pending messages (not yet delivered to any client) OR the number pending acks is exceeded the pull request will return messages.
     * */
    Overflow("overflow"),

    /** The client pull request will specify a priortity from 1 to 10 in the {@link io.nats.client.BaseConsumeOptions} <BR>
     * Request with lower priority will be served first. That is, higher priority request will only be served when no pull request from lower priorities are pending.
     * */
    Prioritized("prioritized"),

    /**
		If multiple clients make requests only ONE will be served messages. The API will identify clients through a UUID. <BR>.
		If a client fails to make requests for more than the timeout specified in {@link io.nats.client.BaseConsumeOptions} another client will be served.
    */
    PinnedClient("pinned_client");

    private final String policy;

    PriorityPolicy(String p) {
        policy = p;
    }

    @Override
    public String toString() {
        return policy;
    }

    private static final Map<String, PriorityPolicy> strEnumHash = new HashMap<>();

    static {
        for (PriorityPolicy env : PriorityPolicy.values()) {
            strEnumHash.put(env.toString(), env);
        }
    }

    /**
     * Get an instance from the JSON value
     * @param value the value
     * @return the instance or null if the string is not matched
     */
    @Nullable
    public static PriorityPolicy get(String value) {
        return strEnumHash.get(value);
    }
}
