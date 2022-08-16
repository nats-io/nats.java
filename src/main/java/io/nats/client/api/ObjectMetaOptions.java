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

import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonUtils;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;

/**
 * The ObjectMeta is Object Meta is high level information about an object
 *
 * THIS IS A PLACEHOLDER FOR THE EXPERIMENTAL OBJECT STORE IMPLEMENTATION.
 */
class ObjectMetaOptions implements JsonSerializable {

    private final String name;
    private final String description;
    private final ObjectMetaOptions objectMetaOptions;

    private ObjectMetaOptions(Builder b) {
        name = b.name;
        description = b.description;
        objectMetaOptions = b.optionsBuilder.build();
    }

    ObjectMetaOptions(String json) {
        name = JsonUtils.readString(json, NAME_RE);
        description = JsonUtils.readString(json, DESCRIPTION_RE);
        objectMetaOptions = ObjectMetaOptions.instance(json);
    }
    static ObjectMetaOptions instance(String json) {
        return new ObjectMetaOptions(json);
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, NAME, name);
        JsonUtils.addField(sb, DESCRIPTION, description);

        // avoid adding an empty child to the json because JsonUtils.addField
        // only checks versus the object being null, which it is never
        if (objectMetaOptions.hasData()) {
            JsonUtils.addField(sb, OPTIONS, objectMetaOptions);
        }

        return endJson(sb).toString();
    }

    boolean hasData() {
        // TODO
        throw new RuntimeException("Not Implemented");
    }

    void embedJson(StringBuilder sb) {
        JsonUtils.addField(sb, NAME, name);
        JsonUtils.addField(sb, DESCRIPTION, description);
        JsonUtils.addField(sb, OPTIONS, objectMetaOptions);
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public ObjectMetaOptions getObjectMetaOptions() {
        return objectMetaOptions;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(ObjectMetaOptions om) {
        return new Builder(om);
    }

    public static ObjectMetaOptions name(String name) {
        return new Builder().name(name).build();
    }

    public ObjectLink getLink() {
        // TODO
        throw new RuntimeException("Not Implemented");
    }

    public static class Builder {
        String name;
        String description;
        Builder optionsBuilder;

        public Builder() {
            this(null);
        }

        public Builder(ObjectMetaOptions om) {
            if (om != null) {
                name = om.name;
                description = om.description;
                optionsBuilder = ObjectMetaOptions.builder(om.objectMetaOptions);
            }
            else {
                optionsBuilder = ObjectMetaOptions.builder();
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

        public Builder options(ObjectMetaOptions objectMetaOptions) {
            optionsBuilder = ObjectMetaOptions.builder(objectMetaOptions);
            return this;
        }

        public Builder chunkSize(int chunkSize) {
            optionsBuilder.chunkSize(chunkSize);
            return this;
        }

        public Builder link(ObjectLink link) {
            optionsBuilder.link(link);
            return this;
        }

        public ObjectMetaOptions build() {
            return new ObjectMetaOptions(this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ObjectMetaOptions that = (ObjectMetaOptions) o;

        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (description != null ? !description.equals(that.description) : that.description != null) return false;
        return objectMetaOptions != null ? objectMetaOptions.equals(that.objectMetaOptions) : that.objectMetaOptions == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (objectMetaOptions != null ? objectMetaOptions.hashCode() : 0);
        return result;
    }
}
