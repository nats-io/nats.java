// Copyright 2021 The NATS Authors
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
import static io.nats.client.support.NatsObjectStoreUtil.DEFAULT_CHUNK_SIZE;
import static io.nats.client.support.Validator.validateGtZeroOrMinus1;

/**
 * The ObjectMetaOptions
 */
public class ObjectMetaOptions implements JsonSerializable {

    private final ObjectLink link;
    private final int chunkSize;

    private ObjectMetaOptions() {
        link = null;
        chunkSize = DEFAULT_CHUNK_SIZE;
    }

    public ObjectMetaOptions(Builder b) {
        link = b.link;
        chunkSize = b.chunkSize;
    }

    public boolean hasData() {
        return link != null || chunkSize != DEFAULT_CHUNK_SIZE;
    }

    static ObjectMetaOptions instance(String fullJson) {
        String objJson = JsonUtils.getJsonObject(OPTIONS, fullJson, null);
        return objJson == null ? new ObjectMetaOptions() : new ObjectMetaOptions(objJson);
    }

    ObjectMetaOptions(String json) {
        link = ObjectLink.optionalInstance(json);
        chunkSize = JsonUtils.readInt(json, MAX_CHUNK_SIZE_RE, DEFAULT_CHUNK_SIZE);
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, LINK, link);
        JsonUtils.addField(sb, MAX_CHUNK_SIZE, chunkSize);
        return endJson(sb).toString();
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

    public static Builder builder(ObjectMetaOptions omo) {
        return new Builder(omo);
    }

    public static ObjectMetaOptions chunkSize(int chunkSize) {
        return new Builder().chunkSize(chunkSize).build();
    }

    public static class Builder {
        ObjectLink link;
        int chunkSize = DEFAULT_CHUNK_SIZE;

        public Builder() {}

        public Builder(ObjectMetaOptions omo) {
            if (omo != null) {
                if (omo.link != null) {
                    link = ObjectLink.builder(omo.link).build();
                }
                chunkSize = omo.chunkSize;
            }
        }

        public Builder link(ObjectLink link) {
            this.link = link;
            return this;
        }

        public Builder chunkSize(int chunkSize) {
            this.chunkSize = (int)validateGtZeroOrMinus1(chunkSize, "Chunk Size");
            if (this.chunkSize == -1) {
                this.chunkSize = DEFAULT_CHUNK_SIZE;
            }
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
        result = 31 * result + (int) (chunkSize ^ (chunkSize >>> 32));
        return result;
    }
}
