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

import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonUtils;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.*;

/**
 * External Information
 */
public class External implements JsonSerializable {
    private final String api;
    private final String deliver;

    public static External optionalInstance(String fullJson) {
        String objJson = JsonUtils.getJsonObject(EXTERNAL, fullJson, null);
        return objJson == null ? null : new External(objJson);
    }

    public External(String json) {
        api = JsonUtils.readString(json, API_RE);
        deliver = JsonUtils.readString(json, DELIVER_RE);
    }

    public External(String api, String deliver) {
        this.api = api;
        this.deliver = deliver;
    }

    /**
     * Returns a JSON representation of this mirror
     *
     * @return json mirror json string
     */
    public String toJson() {
        StringBuilder sb = beginJson();
        addField(sb, API, api);
        addField(sb, DELIVER, deliver);
        return endJson(sb).toString();
    }

    public String getApi() {
        return api;
    }

    public String getDeliver() {
        return deliver;
    }

    @Override
    public String toString() {
        return "External{" +
                "api='" + api + '\'' +
                ", deliver='" + deliver + '\'' +
                '}';
    }
}
