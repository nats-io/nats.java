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
 * OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL.
 */
public class ObjectMetaOptions implements JsonSerializable {

    private final ObjectLink link;
    private final int chunkSize;

    private ObjectMetaOptions(Builder b) {
        link = b.link;
        chunkSize = b.chunkSize;
    }

    ObjectMetaOptions(String json) {
        link = ObjectLink.optionalInstance(json);
        chunkSize = JsonUtils.readInt(json, MAX_CHUNK_SIZE_RE, -1);
    }

    static ObjectMetaOptions instance(String json) {
        return new ObjectMetaOptions(json);
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, LINK, link);
        JsonUtils.addField(sb, MAX_CHUNK_SIZE, chunkSize);
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

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(ObjectMetaOptions om) {
        return new Builder(om);
    }

    public static class Builder {
        ObjectLink link;
        int chunkSize;

        public Builder() {
            this(null);
        }

        public Builder(ObjectMetaOptions om) {
            if (om != null) {
                link = om.link;
                chunkSize = om.chunkSize;
            }
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
