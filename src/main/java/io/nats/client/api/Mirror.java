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

import java.time.ZonedDateTime;

import static io.nats.client.support.ApiConstants.MIRROR;

/**
 * Mirror Information. Maintains a 1:1 mirror of another stream with name matching this property.
 * When a mirror is configured subjects and sources must be empty.
 */
public class Mirror extends SourceBase {

    static Mirror optionalInstance(String fullJson) {
        String objJson = JsonUtils.getJsonObject(MIRROR, fullJson, null);
        return objJson == null ? null : new Mirror(objJson);
    }

    Mirror(String json) {
        super(MIRROR, json);
    }

    Mirror(String name, long startSeq, ZonedDateTime startTime, String filterSubject, External external) {
        super(MIRROR, name, startSeq, startTime, filterSubject, external);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends SourceBaseBuilder<Builder> {
        @Override
        Builder getThis() {
            return this;
        }

        public Mirror build() {
            return new Mirror(sourceName, startSeq, startTime, filterSubject, external);
        }
    }
}
