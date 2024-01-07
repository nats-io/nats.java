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

import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;

public class ConnectOpts implements JsonSerializable {
    public final String jwt;
    public final String nkey;
    public final String sig;
    public final String authToken;
    public final String user;
    public final String pass;
    public final String name;
    public final String lang;
    public final String version;
    public final int protocol;

    static ConnectOpts optionalInstance(JsonValue jv) {
        return jv == null ? null : new ConnectOpts(jv);
    }

    public ConnectOpts(JsonValue jv) {
        jwt = JsonValueUtils.readString(jv, "jwt");
        nkey = JsonValueUtils.readString(jv, "nkey");
        sig = JsonValueUtils.readString(jv, "sig");
        authToken = JsonValueUtils.readString(jv, "auth_token");
        user = JsonValueUtils.readString(jv, "user");
        pass = JsonValueUtils.readString(jv, "pass");
        name = JsonValueUtils.readString(jv, "name");
        lang = JsonValueUtils.readString(jv, "lang");
        version = JsonValueUtils.readString(jv, "version");
        protocol = JsonValueUtils.readInteger(jv, "protocol", -1);
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, "jwt", jwt);
        JsonUtils.addField(sb, "nkey", nkey);
        JsonUtils.addField(sb, "sig", sig);
        JsonUtils.addField(sb, "auth_token", authToken);
        JsonUtils.addField(sb, "user", user);
        JsonUtils.addField(sb, "pass", pass);
        JsonUtils.addField(sb, "name", name);
        JsonUtils.addField(sb, "lang", lang);
        JsonUtils.addField(sb, "version", version);
        JsonUtils.addField(sb, "protocol", protocol);
        return endJson(sb).toString();
    }
}
