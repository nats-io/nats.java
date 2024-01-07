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

import java.time.Duration;

import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;

public class ResponsePermission implements JsonSerializable {
    public int max;
    public Duration expires;

    static ResponsePermission optionalInstance(JsonValue jv) {
        return jv == null ? null : new ResponsePermission(jv);
    }

    public ResponsePermission() {}

    public ResponsePermission(JsonValue jv) {
        max = JsonValueUtils.readInteger(jv, "max", 0);
        expires = JsonValueUtils.readNanos(jv, "ttl");
    }

    public ResponsePermission max(int max) {
        this.max = max;
        return this;
    }

    public ResponsePermission expires(Duration expires) {
        this.expires = expires;
        return this;
    }

    public ResponsePermission expires(long expiresMillis) {
        this.expires = Duration.ofMillis(expiresMillis);
        return this;
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, "max", max);
        JsonUtils.addFieldAsNanos(sb, "ttl", expires);
        return endJson(sb).toString();
    }
}