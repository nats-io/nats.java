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

import static io.nats.client.support.ApiConstants.MIRROR;

/**
 * Mirror Information. Maintains a 1:1 mirror of another stream with name matching this property.
 * When a mirror is configured subjects and sources must be empty.
 */
public class Mirror extends SourceBase {

    static Mirror optionalInstance(JsonValue vMirror) {
        return vMirror == null ? null : new Mirror(vMirror);
    }

    Mirror(JsonValue vMirror) {
        super(MIRROR, vMirror);
    }

    Mirror(Builder b) {
        super(MIRROR, b);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(Mirror mirror) {
        return new Builder(mirror);
    }

    public static class Builder extends SourceBaseBuilder<Builder> {
        @Override
        Builder getThis() {
            return this;
        }

        public Builder() {}

        public Builder(Mirror mirror) {
            super(mirror);
        }

        public Mirror build() {
            return new Mirror(this);
        }
    }
}
