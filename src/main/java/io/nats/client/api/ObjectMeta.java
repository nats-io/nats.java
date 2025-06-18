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
import io.nats.client.support.JsonValue;
import io.nats.client.support.Validator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;
import static io.nats.client.support.JsonValueUtils.*;

/**
 * The ObjectMeta is Object Meta is high level information about an object
 */
public class ObjectMeta implements JsonSerializable {

    private final String objectName;
    private final String description;
    private final Headers headers;
    private final ObjectMetaOptions objectMetaOptions;

    private ObjectMeta(Builder b) {
        objectName = b.objectName;
        description = b.description;
        headers = b.headers;
        objectMetaOptions = b.metaOptionsBuilder.build();
    }

    ObjectMeta(JsonValue vObjectMeta) {
        objectName = readString(vObjectMeta, NAME);
        description = readString(vObjectMeta, DESCRIPTION);
        headers = new Headers();
        JsonValue hJv = readObject(vObjectMeta, HEADERS);
        for (String key : hJv.map.keySet()) {
            headers.put(key, readStringList(hJv, key));
        }

        objectMetaOptions = new ObjectMetaOptions(readObject(vObjectMeta, OPTIONS));
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

    @NotNull
    public String getObjectName() {
        return objectName;
    }

    @Nullable
    public String getDescription() {
        return description;
    }

    @Nullable
    public Headers getHeaders() {
        return headers;
    }

    @Nullable
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
        String objectName;
        String description;
        Headers headers;
        ObjectMetaOptions.Builder metaOptionsBuilder;

        public Builder(String objectName) {
            headers = new Headers();
            metaOptionsBuilder = ObjectMetaOptions.builder();
            objectName(objectName);
        }

        public Builder(ObjectMeta om) {
            objectName = om.objectName;
            description = om.description;
            headers = om.headers;
            metaOptionsBuilder = ObjectMetaOptions.builder(om.objectMetaOptions);
        }

        public Builder objectName(String name) {
            this.objectName = Validator.validateNotNull(name, "Object Name");
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder headers(Headers headers) {
            if (headers == null) {
                this.headers.clear();
            }
            else {
                this.headers = headers;
            }
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

        if (!objectName.equals(that.objectName)) return false;
        if (description != null ? !description.equals(that.description) : that.description != null) return false;
        if (!headers.equals(that.headers)) return false;
        return objectMetaOptions.equals(that.objectMetaOptions);
    }

    @Override
    public int hashCode() {
        int result = objectName.hashCode();
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + headers.hashCode();
        result = 31 * result + objectMetaOptions.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ObjectMeta{" +
            "objectName='" + objectName + '\'' +
            ", description='" + description + '\'' +
            ", headers?" + headers.size() +
            ", objectMetaOptions=" + objectMetaOptions +
            '}';
    }
}
