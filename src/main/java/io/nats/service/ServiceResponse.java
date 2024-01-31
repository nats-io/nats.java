// Copyright 2023 The NATS Authors
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

package io.nats.service;

import io.nats.client.support.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonValueUtils.readString;
import static io.nats.client.support.JsonValueUtils.readStringStringMap;
import static io.nats.client.support.JsonWriteUtils.*;

/**
 * Base class for service responses Info, Ping and Stats
 */
public abstract class ServiceResponse implements JsonSerializable {
    protected final String type;
    protected final String name;
    protected final String id;
    protected final String version;
    protected final Map<String, String> metadata;

    protected ServiceResponse(String type, String id, String name, String version, Map<String, String> metadata) {
        this.type = type;
        this.id = id;
        this.name = name;
        this.version = version;
        this.metadata = metadata == null || metadata.isEmpty() ? null : metadata;
    }

    protected ServiceResponse(String type, ServiceResponse template) {
        this(type, template.id, template.name, template.version, template.metadata);
    }

    protected ServiceResponse(String type, JsonValue jv) {
        String jvType = readString(jv, TYPE);
        if (Validator.emptyAsNull(jvType) == null) {
            throw new IllegalArgumentException("Type cannot be null or empty.");
        }
        if (!type.equals(jvType)) {
            throw new IllegalArgumentException("Invalid type for " + getClass().getSimpleName() + ". Expecting: " + type + ". Received " + jvType);
        }
        this.type = type;
        id = Validator.required(readString(jv, ID), "Id");
        name = Validator.required(readString(jv, NAME), "Name");
        version = Validator.required(readString(jv, VERSION), "Version");
        metadata = readStringStringMap(jv, METADATA);
    }

    protected static JsonValue parseMessage(byte[] bytes) {
        try {
            return JsonParser.parse(bytes);
        }
        catch (JsonParseException e) {
            return JsonValue.EMPTY_MAP;
        }
    }

    /**
     * The type of this response;
     * @return the type string
     */
    public String getType() {
        return type;
    }

    /**
     * The unique ID of the service
     * @return the service id
     */
    public String getId() {
        return id;
    }

    /**
     * The name of the service
     * @return the service name
     */
    public String getName() {
        return name;
    }

    /**
     * Version of the service
     * @return the version
     */
    public String getVersion() {
        return version;
    }

    /**
     * A copy of the metadata for the service, or null if there is no metadata
     * @return the metadata
     */
    public Map<String, String> getMetadata() {
        return metadata == null ? null : new HashMap<>(metadata);
    }

    protected void subToJson(StringBuilder sb) {}

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        addField(sb, ID, id);
        addField(sb, NAME, name);
        addField(sb, VERSION, version);
        subToJson(sb);
        addField(sb, TYPE, type);
        addField(sb, METADATA, metadata);
        return endJson(sb).toString();
    }


    @Override
    public String toString() {
        return toKey(getClass()) + toJson();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ServiceResponse that = (ServiceResponse) o;

        if (!Objects.equals(type, that.type)) return false;
        if (!Objects.equals(name, that.name)) return false;
        if (!Objects.equals(id, that.id)) return false;
        if (!Objects.equals(version, that.version)) return false;
        return Validator.mapEquals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (version != null ? version.hashCode() : 0);
        result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
        return result;
    }
}
