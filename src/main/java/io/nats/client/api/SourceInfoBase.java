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
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.List;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonValueUtils.*;

abstract class SourceInfoBase {
    protected final JsonValue jv;
    protected final String name;
    protected final long lag;
    protected final Duration active;
    protected final External external;
    protected final List<SubjectTransform> subjectTransforms;
    protected final Error error;

    SourceInfoBase(JsonValue vSourceInfo) {
        jv = vSourceInfo;
        name = readString(vSourceInfo, NAME);
        lag = readLong(vSourceInfo, LAG, 0);
        active = readNanos(vSourceInfo, ACTIVE, Duration.ZERO);
        external = External.optionalInstance(readValue(vSourceInfo, EXTERNAL));
        subjectTransforms = SubjectTransform.optionalListOf(readValue(vSourceInfo, SUBJECT_TRANSFORMS));
        error = Error.optionalInstance(readValue(vSourceInfo, ERROR));
    }

    /**
     * The name of the Stream being replicated
     * @return the name
     */
    @NonNull
    public String getName() {
        return name;
    }

    /**
     * How many uncommitted operations this peer is behind the leader
     * @return the lag
     */
    public long getLag() {
        return lag;
    }

    /**
     * Time since this peer was last seen
     * @return the time
     */
    @NonNull
    public Duration getActive() {
        return active;
    }

    /**
     * Configuration referencing a stream source in another account or JetStream domain
     * @return the external
     */
    @Nullable
    public External getExternal() {
        return external;
    }

    /**
     * The list of subject transforms, if any
     * @return the list of subject transforms
     */
    @Nullable
    public List<SubjectTransform> getSubjectTransforms() {
        return subjectTransforms;
    }

    /**
     * The last error
     * @return the error
     */
    @Nullable
    public Error getError() {
        return error;
    }
}
