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
 * The ObjectLink is used to embed links to other objects.
 *
 * OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL.
 */
public class ObjectLink implements JsonSerializable {

    private final String bucket;
    private final String name;

    static ObjectLink optionalInstance(String fullJson) {
        String objJson = JsonUtils.getJsonObject(LINK, fullJson, null);
        return objJson == null ? null : new ObjectLink(objJson);
    }

    ObjectLink(String json) {
        bucket = JsonUtils.readString(json, BUCKET_RE);
        name = JsonUtils.readString(json, NAME_RE);
    }

    private ObjectLink(Builder b) {
        this.bucket = b.bucket;
        this.name = b.name;
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, BUCKET, bucket);
        JsonUtils.addField(sb, NAME, name);
        return endJson(sb).toString();
    }

    public String getBucket() {
        return bucket;
    }

    public String getName() {
        return name;
    }

    public boolean isObjectLink() {
        return name != null;
    }

    public boolean isBucketLink() {
        return name == null;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(ObjectLink link) {
        return new Builder(link);
    }

    public static class Builder {
        private String bucket;
        private String name;

        public Builder() {}

        public Builder(ObjectLink link) {
            if (link != null) {
                bucket = link.bucket;
                name = link.name;
            }
        }

        public Builder bucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public ObjectLink build() {
            return new ObjectLink(this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ObjectLink that = (ObjectLink) o;

        if (bucket != null ? !bucket.equals(that.bucket) : that.bucket != null) return false;
        return name != null ? name.equals(that.name) : that.name == null;
    }

    @Override
    public int hashCode() {
        int result = bucket != null ? bucket.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ObjectLink{" +
            "bucket='" + bucket + '\'' +
            ", name='" + name + '\'' +
            '}';
    }
}
