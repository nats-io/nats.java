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
 * External configuration referencing a stream source in another account
 */
public class External implements JsonSerializable {
    private final String api;
    private final String deliver;

    static External optionalInstance(String fullJson) {
        String objJson = JsonUtils.getJsonObject(EXTERNAL, fullJson, null);
        return objJson == null ? null : new External(objJson);
    }

    External(String json) {
        api = JsonUtils.readString(json, API_RE);
        deliver = JsonUtils.readString(json, DELIVER_RE);
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

    /**
     * The subject prefix that imports the other account <code>$JS.API.CONSUMER.&gt; subjects</code>
     *
     * @return the api prefix
     */
    public String getApi() {
        return api;
    }

    /**
     * The delivery subject to use for the push consumer.
     *
     * @return delivery subject
     */
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        External external = (External) o;

        if (api != null ? !api.equals(external.api) : external.api != null) return false;
        return deliver != null ? deliver.equals(external.deliver) : external.deliver == null;
    }

    @Override
    public int hashCode() {
        int result = api != null ? api.hashCode() : 0;
        result = 31 * result + (deliver != null ? deliver.hashCode() : 0);
        return result;
    }
}
