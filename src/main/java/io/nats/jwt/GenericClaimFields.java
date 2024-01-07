// Copyright 2021-2024 The NATS Authors
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

package io.nats.jwt;

import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonUtils;
import io.nats.client.support.JsonValue;
import io.nats.client.support.JsonValueUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class GenericClaimFields<B> implements JsonSerializable {
    public List<String> tags;
    public String type;
    public int version;

    protected GenericClaimFields(String type, int version) {
        this.type = type;
        this.version = version;
    }

    protected GenericClaimFields(JsonValue jv, String expectedType, int... validVersions) {
        type = JsonValueUtils.readString(jv, "type");
        if (!type.equals(expectedType)) {
            throw new IllegalArgumentException("Invalid Claim Type '" + type + "', expecting '" + expectedType + "'");
        }

        tags = JsonValueUtils.readOptionalStringList(jv, "tags");

        version = JsonValueUtils.readInteger(jv, "version", -1);
        for (int v : validVersions) {
            if (version == v) {
                return;
            }
        }
        throw new IllegalArgumentException("Invalid Version '" + version + "'");
    }

    public String getType() {
        return type;
    }

    protected void baseJson(StringBuilder sb) {
        JsonUtils.addStrings(sb, "tags", tags);
        JsonUtils.addField(sb, "type", type);
        JsonUtils.addField(sb, "version", version);
    }

    protected abstract B getThis();

    public B tags(String... tags) {
        if (tags == null) {
            this.tags = null;
        }
        else {
            this.tags = new ArrayList<>(Arrays.asList(tags));
        }
        return getThis();
    }

    public B tags(List<String> tags) {
        this.tags = tags;
        return getThis();
    }
}