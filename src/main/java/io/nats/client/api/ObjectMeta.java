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
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

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
    private final Map<String, String> metadata;
    private final ObjectMetaOptions objectMetaOptions;

    private ObjectMeta(Builder b) {
        objectName = b.objectName;
        description = b.description;
        headers = new Headers(b.headers, true);
        metadata = Collections.unmodifiableMap(b.metadata);
        objectMetaOptions = b.metaOptionsBuilder.build();
    }

    ObjectMeta(JsonValue vObjectMeta) {
        objectName = readString(vObjectMeta, NAME);
        description = readString(vObjectMeta, DESCRIPTION);
        Headers h = new Headers();
        JsonValue hJv = readObject(vObjectMeta, HEADERS);
        for (String key : hJv.map.keySet()) {
            h.put(key, readStringList(hJv, key));
        }
        headers = new Headers(h, true);
        Map<String, String> meta = readStringStringMap(vObjectMeta, METADATA);
        metadata = meta == null ? Collections.unmodifiableMap(new HashMap<>()) : Collections.unmodifiableMap(meta);
        objectMetaOptions = new ObjectMetaOptions(readObject(vObjectMeta, OPTIONS));
    }

    @Override
    @NonNull
    public String toJson() {
        StringBuilder sb = beginJson();
        embedJson(sb);
        return endJson(sb).toString();
    }

    void embedJson(StringBuilder sb) {
        JsonUtils.addField(sb, NAME, objectName);
        JsonUtils.addField(sb, DESCRIPTION, description);
        JsonUtils.addField(sb, HEADERS, headers);
        JsonUtils.addField(sb, METADATA, metadata);

        // avoid adding an empty child to the json because JsonUtils.addField
        // only checks versus the object being null, which it is never
        if (objectMetaOptions.hasData()) {
            JsonUtils.addField(sb, OPTIONS, objectMetaOptions);
        }
    }

    /**
     * The object name
     * @return the object name
     */
    @NonNull
    public String getObjectName() {
        return objectName;
    }

    /**
     * The description
     * @return the description text or null
     */
    @Nullable
    public String getDescription() {
        return description;
    }

    /**
     * Headers may be empty but will not be null. In all cases it will be unmodifiable
     * @return the headers object
     */
    @NonNull
    public Headers getHeaders() {
        return headers;
    }

    /**
     * Metadata may be empty but will not be null. In all cases it will be unmodifiable
     * @return the map
     */
    @NonNull
    public Map<String, String> getMetadata() {
        return metadata;
    }

    /**
     * The ObjectMetaOptions are additional options describing the object
     * @return the object meta data
     */
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
        Map<String, String> metadata;
        ObjectMetaOptions.Builder metaOptionsBuilder;

        public Builder(String objectName) {
            headers = new Headers();
            metadata = new HashMap<>();
            metaOptionsBuilder = ObjectMetaOptions.builder();
            objectName(objectName);
        }

        public Builder(ObjectMeta om) {
            objectName = om.objectName;
            description = om.description;
            headers = new Headers(om.headers);
            metadata = new HashMap<>(om.metadata);
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
                this.headers = new Headers(headers);
            }
            return this;
        }

        public Builder metadata(Map<String, String> metadata) {
            if (metadata == null) {
                this.metadata.clear();
            }
            else {
                this.metadata = metadata;
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
        if (!Objects.equals(headers, that.headers)) return false;
        if (!Objects.equals(metadata, that.metadata)) return false;
        return objectMetaOptions.equals(that.objectMetaOptions);
    }

    @Override
    public int hashCode() {
        int result = objectName.hashCode();
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + headers.hashCode();
        result = 31 * result + metadata.hashCode();
        result = 31 * result + objectMetaOptions.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ObjectMeta{" +
            "objectName='" + objectName + '\'' +
            ", description='" + description + '\'' +
            ", headers?" + headers.size() +
            ", metadata?" + metadata.size() +
            ", objectMetaOptions=" + objectMetaOptions +
            '}';
    }
}
