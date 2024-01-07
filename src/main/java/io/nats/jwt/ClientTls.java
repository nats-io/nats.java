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

public class ClientTls implements JsonSerializable {
    public final String version;
    public final String cipher;
    public final List<String> certs;
    public final List<List<String>> verifiedChains;

    static ClientTls optionalInstance(JsonValue jv) {
        return jv == null ? null : new ClientTls(jv);
    }

    public ClientTls(JsonValue jv) {
        version = JsonValueUtils.readString(jv, "version");
        cipher = JsonValueUtils.readString(jv, "cipher");
        certs = JsonValueUtils.readOptionalStringList(jv, "certs");
        verifiedChains = null; // verified_chains
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, "version", version);
        JsonUtils.addField(sb, "protocol", cipher);
        JsonUtils.addStrings(sb, "tags", certs);
        return endJson(sb).toString();
    }
}
