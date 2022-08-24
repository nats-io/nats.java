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
package io.nats.client.api;

import io.nats.client.impl.Headers;
import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonUtils;

import java.util.Map;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;

/**
 * The ObjectMeta is Object Meta is high level information about an object
 *
 * OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL.
 */
public class ObjectMeta implements JsonSerializable {

    private final String name;
    private final String description;
    private final Headers headers;
    private final ObjectMetaOptions objectMetaOptions;

    private ObjectMeta(Builder b) {
        name = b.name;
        description = b.description;
        headers = b.headers;
        objectMetaOptions = b.metaOptionsBuilder.build();
    }

    ObjectMeta(String json) {
        name = JsonUtils.readString(json, NAME_RE);
        description = JsonUtils.readString(json, DESCRIPTION_RE);
        String headersJson = JsonUtils.getJsonObject(HEADERS, json);
        headers = new Headers();
        if (headersJson != null) {
            Map<String, String>  map = JsonUtils.getMapOfObjects(headersJson);
            // TODO
        }
        objectMetaOptions = ObjectMetaOptions.instance(json);
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        embedJson(sb);
        return endJson(sb).toString();
    }

    void embedJson(StringBuilder sb) {
        JsonUtils.addField(sb, NAME, name);
        JsonUtils.addField(sb, DESCRIPTION, description);
        JsonUtils.addField(sb, HEADERS, headers);

        // avoid adding an empty child to the json because JsonUtils.addField
        // only checks versus the object being null, which it is never
        if (objectMetaOptions.hasData()) {
            JsonUtils.addField(sb, OPTIONS, objectMetaOptions);
        }
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public Headers getHeaders() {
        return headers;
    }

    public ObjectMetaOptions getObjectMetaOptions() {
        return objectMetaOptions;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(ObjectMeta om) {
        return new Builder(om);
    }

    public static ObjectMeta name(String name) {
        return new Builder().name(name).build();
    }

    public static class Builder {
        String name;
        String description;
        Headers headers;
        ObjectMetaOptions.Builder metaOptionsBuilder;

        public Builder() {
            this(null);
        }

        public Builder(ObjectMeta om) {
            if (om != null) {
                name = om.name;
                description = om.description;
                headers = om.headers;
                metaOptionsBuilder = ObjectMetaOptions.builder(om.objectMetaOptions);
            }
            else {
                metaOptionsBuilder = ObjectMetaOptions.builder();
            }
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder headers(Headers headers) {
            this.headers = headers;
            return this;
        }

        public Builder options(ObjectMetaOptions objectMetaOptions) {
            metaOptionsBuilder = ObjectMetaOptions.builder(objectMetaOptions);
            return this;
        }

        public Builder chunkSize(int chunkSize) {
            metaOptionsBuilder.chunkSize(chunkSize);
            return this;
        }

        public Builder link(ObjectLink link) {
            metaOptionsBuilder.link(link);
            return this;
        }

        public ObjectMeta build() {
            return new ObjectMeta(this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ObjectMeta that = (ObjectMeta) o;

        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (description != null ? !description.equals(that.description) : that.description != null) return false;
        if (headers != null ? !headers.equals(that.headers) : that.headers != null) return false;
        return objectMetaOptions != null ? objectMetaOptions.equals(that.objectMetaOptions) : that.objectMetaOptions == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (headers != null ? headers.hashCode() : 0);
        result = 31 * result + (objectMetaOptions != null ? objectMetaOptions.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ObjectMeta{" +
            "name='" + name + '\'' +
            ", description='" + description + '\'' +
            ", objectMetaOptions=" + objectMetaOptions +
            '}';
    }
}
