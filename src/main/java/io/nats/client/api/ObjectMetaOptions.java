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
import io.nats.client.support.JsonValue;
import io.nats.client.support.JsonWriteUtils;

import static io.nats.client.support.ApiConstants.LINK;
import static io.nats.client.support.ApiConstants.MAX_CHUNK_SIZE;
import static io.nats.client.support.JsonValueUtils.readInteger;
import static io.nats.client.support.JsonValueUtils.readValue;
import static io.nats.client.support.JsonWriteUtils.beginJson;
import static io.nats.client.support.JsonWriteUtils.endJson;

/**
 * The ObjectMeta is Object Meta is high level information about an object.
 */
public class ObjectMetaOptions implements JsonSerializable {

    private final ObjectLink link;
    private final int chunkSize;

    private ObjectMetaOptions(Builder b) {
        link = b.link;
        chunkSize = b.chunkSize;
    }

    ObjectMetaOptions(JsonValue vOptions) {
        link = ObjectLink.optionalInstance(readValue(vOptions, LINK));
        chunkSize = readInteger(vOptions, MAX_CHUNK_SIZE, -1);
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonWriteUtils.addField(sb, LINK, link);
        JsonWriteUtils.addField(sb, MAX_CHUNK_SIZE, chunkSize);
        return endJson(sb).toString();
    }

    boolean hasData() {
        return link != null || chunkSize > 0;
    }

    public ObjectLink getLink() {
        return link;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    static Builder builder() {
        return new Builder();
    }

    static Builder builder(ObjectMetaOptions om) {
        return new Builder(om);
    }

    public static class Builder {
        ObjectLink link;
        int chunkSize;

        public Builder() {}

        public Builder(ObjectMetaOptions om) {
            link = om.link;
            chunkSize = om.chunkSize;
        }

        public Builder link(ObjectLink link) {
            this.link = link;
            return this;
        }

        public Builder chunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
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

        ObjectMetaOptions options = (ObjectMetaOptions) o;

        if (chunkSize != options.chunkSize) return false;
        return link != null ? link.equals(options.link) : options.link == null;
    }

    @Override
    public int hashCode() {
        int result = link != null ? link.hashCode() : 0;
        result = 31 * result + chunkSize;
        return result;
    }

    @Override
    public String toString() {
        return "ObjectMetaOptions{" +
            "link=" + link +
            ", chunkSize=" + chunkSize +
            '}';
    }
}
