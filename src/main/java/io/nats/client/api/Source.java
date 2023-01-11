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

import java.util.ArrayList;
import java.util.List;

import static io.nats.client.support.ApiConstants.SOURCE;

/**
 * Source Information
 */
public class Source extends SourceBase {
    static List<Source> optionalListOf(JsonValue vSources) {
        if (vSources == null || vSources.array == null || vSources.array.size() == 0) {
            return null;
        }

        List<Source> list = new ArrayList<>();
        for (JsonValue jv : vSources.array) {
            list.add(new Source(jv));
        }
        return list;
    }

    Source(JsonValue vSource) {
        super(SOURCE, vSource);
    }

    Source(Builder b) {
        super(SOURCE, b);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(Source source) {
        return new Builder(source);
    }

    public static class Builder extends SourceBaseBuilder<Builder> {
        @Override
        Builder getThis() {
            return this;
        }

        public Builder() {}

        public Builder(Source source) {
            super(source);
        }

        public Source build() {
            return new Source(this);
        }
    }
}
