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

import java.util.List;

import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;

public class ServerId implements JsonSerializable {
    public final String name;
    public final String host;
    public final String id;
    public final String version;
    public final String cluster;
    public final List<String> tags;
    public final String xKey;

    static ServerId optionalInstance(JsonValue jv) {
        return jv == null ? null : new ServerId(jv);
    }

    public ServerId(JsonValue jv) {
        name = JsonValueUtils.readString(jv, "name");
        host = JsonValueUtils.readString(jv, "host");
        id = JsonValueUtils.readString(jv, "id");
        version = JsonValueUtils.readString(jv, "version");
        cluster = JsonValueUtils.readString(jv, "cluster");
        tags = JsonValueUtils.readOptionalStringList(jv, "tags");
        xKey = JsonValueUtils.readString(jv, "xKey");
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, "name", name);
        JsonUtils.addField(sb, "host", host);
        JsonUtils.addField(sb, "id", id);
        JsonUtils.addField(sb, "version", version);
        JsonUtils.addField(sb, "cluster", cluster);
        JsonUtils.addStrings(sb, "tags", tags);
        JsonUtils.addField(sb, "xKey", xKey);
        return endJson(sb).toString();
    }
}
