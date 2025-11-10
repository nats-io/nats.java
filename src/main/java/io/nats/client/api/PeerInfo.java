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
import org.jspecify.annotations.NonNull;

import java.time.Duration;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonValueUtils.*;

/**
 * Server peer information
 */
public class PeerInfo {

    private final String name;
    private final boolean current;
    private final boolean offline;
    private final Duration active;
    private final long lag;

    PeerInfo(JsonValue vPeerInfo) {
        name = readString(vPeerInfo, NAME);
        current = readBoolean(vPeerInfo, CURRENT);
        offline = readBoolean(vPeerInfo, OFFLINE);
        active = readNanos(vPeerInfo, ACTIVE, Duration.ZERO);
        lag = readLong(vPeerInfo, LAG, 0);
    }

    /**
     * The server name of the peer
     * @return the name
     */
    @NonNull
    public String getName() {
        return name;
    }

    /**
     * Indicates if the server is up-to-date and synchronised
     * @return if is current
     */
    public boolean isCurrent() {
        return current;
    }

    /**
     * Indicates the node is considered offline by the group
     * @return if is offline
     */
    public boolean isOffline() {
        return offline;
    }

    /**
     * Time since this peer was last seen
     * @return the active time
     */
    @NonNull
    public Duration getActive() {
        return active;
    }

    /**
     * How many uncommitted operations this peer is behind the leader
     * @return the lag
     */
    public long getLag() {
        return lag;
    }
}
