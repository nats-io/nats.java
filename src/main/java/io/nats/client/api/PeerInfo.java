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

import io.nats.client.support.JsonValue;

import java.time.Duration;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonValueUtils.*;

public class PeerInfo {

    private final String name;
    private final boolean current;
    private final boolean offline;
    private final Duration active;
    private final long lag;

    PeerInfo(JsonValue vPeerInfo) {
        if (vPeerInfo == null) {
            throw new IllegalArgumentException("Cannot construct PeerInfo without a value.");
        }
        name = getMappedString(vPeerInfo, NAME);
        current = getMappedBoolean(vPeerInfo, CURRENT);
        offline = getMappedBoolean(vPeerInfo, OFFLINE);
        active = getMappedNanos(vPeerInfo, ACTIVE, Duration.ZERO);
        lag = getMappedLong(vPeerInfo, LAG, 0);
    }

    public String getName() {
        return name;
    }

    public boolean isCurrent() {
        return current;
    }

    public boolean isOffline() {
        return offline;
    }

    public Duration getActive() {
        return active;
    }

    public long getLag() {
        return lag;
    }
}
