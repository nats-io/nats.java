// Copyright 2022 The NATS Authors
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

package io.nats.service.api;

import io.nats.client.support.*;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.endJson;
import static io.nats.client.support.JsonUtils.toKey;
import static io.nats.client.support.JsonValueUtils.readString;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public abstract class ServiceResponse implements JsonSerializable {
    protected final String type;
    protected final String name;
    protected final String id;
    protected final String version;
    protected final JsonValue jv;

    public ServiceResponse(String type, String id, String name, String version) {
        this.type = type;
        this.id = id;
        this.name = name;
        this.version = version;
        jv = null;
    }

    public ServiceResponse(String type, ServiceResponse template) {
        this.type = type;
        this.id = template.id;
        this.name = template.name;
        this.version = template.version;
        jv = null;
    }

    public ServiceResponse(String type, byte[] jsonBytes) {
        jv = JsonParser.parse(jsonBytes);
        this.type = type;
        String jvType = readString(jv, TYPE);
        if (!type.equals(jvType)) {
            throw new IllegalArgumentException("Invalid type for " + getClass().getSimpleName() + ". Expecting: " + type + ". Received " + jvType);
        }
        name = readString(jv, NAME);
        id = readString(jv, ID);
        version = readString(jv, VERSION);
    }

    /**
     * The type of this response;
     * @return the type string
     */
    public String getType() {
        return type;
    }

    /**
     * The kind of the service reporting the status
     * @return the service name
     */
    public String getName() {
        return name;
    }

    /**
     * The unique ID of the service reporting the status
     * @return the service id
     */
    public String getId() {
        return id;
    }

    /**
     * Version of the service
     * @return the version
     */
    public String getVersion() {
        return version;
    }

    protected abstract void subToJson(StringBuilder sb, boolean forToString);

    private String _toJson(boolean forToString) {
        StringBuilder sb;
        if (forToString) {
            sb = JsonUtils.beginJsonPrefixed(toKey(this.getClass()));
        }
        else {
            sb = JsonUtils.beginJson();
        }
        JsonUtils.addField(sb, NAME, name);
        JsonUtils.addField(sb, ID, id);
        JsonUtils.addField(sb, VERSION, version);
        subToJson(sb, forToString);
        JsonUtils.addField(sb, ApiConstants.TYPE, type);
        return endJson(sb).toString();
    }

    @Override
    public String toJson() {
        return _toJson(false);
    }

    @Override
    public String toString() {
        return _toJson(true);
    }
}
