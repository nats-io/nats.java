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

import io.nats.client.support.JsonUtils;

import java.time.Duration;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.normalize;

public class PeerInfo {
    private final String name;
    private final boolean current;
    private final boolean offline;
    private final Duration active;
    private final long lag;
    private final String objectName;

    PeerInfo(String objectName, String json) {
        name = JsonUtils.readString(json, NAME_RE);
        current = JsonUtils.readBoolean(json, CURRENT_RE);
        offline = JsonUtils.readBoolean(json, OFFLINE_RE);
        active = JsonUtils.readNanos(json, ACTIVE_RE, Duration.ZERO);
        lag = JsonUtils.readLong(json, LAG_RE, 0);
        this.objectName = normalize(objectName);
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

    @Override
    public String toString() {
        return objectName + "{" +
                "name='" + name + '\'' +
                ", current=" + current +
                ", offline=" + offline +
                ", active=" + active +
                ", lag=" + lag +
                '}';
    }
}
