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

package io.nats.client.api;

import io.nats.client.support.JsonValue;
import io.nats.client.support.JsonValueUtils;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.ZonedDateTime;
import java.util.List;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonValueUtils.readDate;

/**
 * Status of a specific consumer priority group
 */
public class PriorityGroupState {
    private final String group;
    private final String pinnedClientId;
    private final ZonedDateTime pinnedTime;

    static List<PriorityGroupState> optionalListOf(JsonValue vpgStates) {
        return JsonValueUtils.optionalListOf(vpgStates, PriorityGroupState::new);
    }

    PriorityGroupState(JsonValue vpgState) {
        group = JsonValueUtils.readString(vpgState, GROUP);
        pinnedClientId = JsonValueUtils.readString(vpgState, PINNED_CLIENT_ID);
        pinnedTime = readDate(vpgState, PINNED_TS);
    }

    /**
     * The group this status is for
     * @return the group
     */
    @NonNull
    public String getGroup() {
        return group;
    }

    /**
     * The generated ID of the pinned client
     * @return the id
     */
    @Nullable
    public String getPinnedClientId() {
        return pinnedClientId;
    }

    /**
     * The timestamp when the client was pinned
     * @return the timestamp
     */
    public ZonedDateTime getPinnedTime() {
        return pinnedTime;
    }

    @Override
    public String toString() {
        return "PriorityGroupState{" +
            "group='" + group + '\'' +
            ", pinnedClientId='" + pinnedClientId + '\'' +
            ", pinnedTime=" + pinnedTime +
            '}';
    }
}
