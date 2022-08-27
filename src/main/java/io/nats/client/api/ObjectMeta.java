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
import io.nats.client.support.Validator;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;

/**
 * The ObjectMeta is Object Meta is high level information about an object
 *
 * OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL.
 */
public class ObjectMeta implements JsonSerializable {

    private final String objectName;
    private final String description;
    private final Headers headers;
    private final ObjectMetaOptions objectMetaOptions;

    private ObjectMeta(Builder b) {
        objectName = b.name;
        description = b.description;
        headers = b.headers;
        objectMetaOptions = b.metaOptionsBuilder.build();
    }

    ObjectMeta(String json) {
        objectName = JsonUtils.readString(json, NAME_RE);
        description = JsonUtils.readString(json, DESCRIPTION_RE);
        String headersJson = JsonUtils.getJsonObject(HEADERS, json);
        headers = new Headers();
        if (headersJson != null) {
            headers.put(JsonUtils.getMapOfLists(headersJson));
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
        JsonUtils.addField(sb, NAME, objectName);
        JsonUtils.addField(sb, DESCRIPTION, description);
        JsonUtils.addField(sb, HEADERS, headers);

        // avoid adding an empty child to the json because JsonUtils.addField
        // only checks versus the object being null, which it is never
        if (objectMetaOptions.hasData()) {
            JsonUtils.addField(sb, OPTIONS, objectMetaOptions);
        }
    }

    public String getObjectName() {
        return objectName;
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

    public static Builder builder(String objectName) {
        return new Builder(objectName);
    }

    public static Builder builder(ObjectMeta om) {
        return new Builder(om);
    }

    public static ObjectMeta objectName(String objectName) {
        return new Builder(objectName).build();
    }

    public static class Builder {
        String name;
        String description;
        Headers headers;
        ObjectMetaOptions.Builder metaOptionsBuilder;

        public Builder(String objectName) {
            metaOptionsBuilder = ObjectMetaOptions.builder();
            objectName(objectName);
        }

        public Builder(ObjectMeta om) {
            name = om.objectName;
            description = om.description;
            headers = om.headers;
            metaOptionsBuilder = ObjectMetaOptions.builder(om.objectMetaOptions);
        }

        public Builder objectName(String name) {
            Validator.validateNotNull(name, "Object Name");
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

        if (objectName != null ? !objectName.equals(that.objectName) : that.objectName != null) return false;
        if (description != null ? !description.equals(that.description) : that.description != null) return false;
        if (headers != null ? !headers.equals(that.headers) : that.headers != null) return false;
        return objectMetaOptions != null ? objectMetaOptions.equals(that.objectMetaOptions) : that.objectMetaOptions == null;
    }

    @Override
    public int hashCode() {
        int result = objectName != null ? objectName.hashCode() : 0;
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (headers != null ? headers.hashCode() : 0);
        result = 31 * result + (objectMetaOptions != null ? objectMetaOptions.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ObjectMeta{" +
            "name='" + objectName + '\'' +
            ", description='" + description + '\'' +
            ", objectMetaOptions=" + objectMetaOptions +
            '}';
    }
}
