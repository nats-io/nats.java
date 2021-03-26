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
import static io.nats.client.support.JsonUtils.*;

abstract class SourceInfoBase {
    private final String name;
    private final long lag;
    private final Duration active;
    private final Error error;
    private final String objectName;

    SourceInfoBase(String json, String objectName) {
        name = readString(json, NAME_RE);
        lag = JsonUtils.readLong(json, LAG_RE, 0);
        active = JsonUtils.readNanos(json, ACTIVE_RE, Duration.ZERO);
        error = Error.optionalInstance(json);
        this.objectName = normalize(objectName);
    }

    /**
     * The name of the Stream being replicated
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    public long getLag() {
        return lag;
    }

    public Duration getActive() {
        return active;
    }

    public Error getError() {
        return error;
    }

    @Override
    public String toString() {
        return objectName + "{" +
                "name='" + name + '\'' +
                ", lag=" + lag +
                ", active=" + active +
                ", " + objectString("error", error) +
                '}';
    }
}
