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

import io.nats.client.Message;
import io.nats.client.support.DateTimeUtils;
import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonUtils;

import java.time.ZonedDateTime;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.*;

/**
 * The ObjectInfo is Object Meta Information plus instance information
 *
 * THIS IS A PLACEHOLDER FOR THE EXPERIMENTAL OBJECT STORE IMPLEMENTATION.
 */
class ObjectInfo implements JsonSerializable {
    private final String bucket;
    private final String nuid;
    private final long size;
    private final ZonedDateTime modified;
    private final long chunks;
    private final String digest;
    private final boolean deleted;
    private final ObjectMeta objectMeta;

    private ObjectInfo(Builder b) {
        bucket = b.bucket;
        nuid = b.nuid;
        size = b.size;
        modified = b.modified;
        chunks = b.chunks;
        digest = b.digest;
        deleted = b.deleted;
        objectMeta = b.metaBuilder.build();
    }

    public ObjectInfo(MessageInfo mi) {
        this(mi.getData() == null ? EMPTY_JSON : new String(mi.getData()), mi.getTime());
    }

    public ObjectInfo(Message m) {
        this(m.getData() == null ? EMPTY_JSON : new String(m.getData()), m.metaData().timestamp());
    }

    ObjectInfo(String json, ZonedDateTime modified) {
        objectMeta = new ObjectMeta(json);

        // we do this (remove the options field) b/c options has a field called bucket
        // and it will mess up the reading of the Object info's bucket field
        json = removeObject(json, OPTIONS);

        bucket = JsonUtils.readString(json, BUCKET_RE);
        nuid = JsonUtils.readString(json, NUID_RE);
        size = JsonUtils.readLong(json, SIZE_RE, 0);
        this.modified = modified;
        chunks = JsonUtils.readLong(json, CHUNKS_RE, 0);
        digest = JsonUtils.readString(json, DIGEST_RE);
        deleted = JsonUtils.readBoolean(json, DELETED_RE);
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();

        // the go code embeds the meta object's fields instead of a child object.
        objectMeta.embedJson(sb);

        JsonUtils.addField(sb, BUCKET, bucket);
        JsonUtils.addField(sb, NUID, nuid);
        JsonUtils.addField(sb, SIZE, size);
        // modified is never stored
        JsonUtils.addField(sb, CHUNKS, chunks);
        JsonUtils.addField(sb, DIGEST, digest);
        JsonUtils.addField(sb, DELETED, deleted);
        return endJson(sb).toString();
    }

    public String getBucket() {
        return bucket;
    }

    public String getNuid() {
        return nuid;
    }

    public long getSize() {
        return size;
    }

    public ZonedDateTime getModified() {
        return modified;
    }

    public long getChunks() {
        return chunks;
    }

    public String getDigest() {
        return digest;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public ObjectMeta getObjectMeta() {
        return objectMeta;
    }

    public String getName() {
        return objectMeta.getName();
    }

    public String getDescription() {
        return objectMeta.getDescription();
    }

    public ObjectMetaOptions getObjectMetaOptions() {
        return objectMeta.getObjectMetaOptions();
    }

    public boolean isLink() {
        return objectMeta.getObjectMetaOptions().getLink() != null;
    }

    public ObjectLink getLink() {
        return objectMeta.getObjectMetaOptions().getLink();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(ObjectInfo info) {
        return new Builder(info);
    }

    public static class Builder {
        String bucket;
        String nuid;
        long size;
        ZonedDateTime modified;
        long chunks;
        String digest;
        boolean deleted;
        ObjectMeta.Builder metaBuilder;

        public Builder() {
            this(null);
        }

        public Builder(ObjectInfo info) {
            if (info != null) {
                bucket = info.bucket;
                nuid = info.nuid;
                size = info.size;
                modified = info.modified;
                chunks = info.chunks;
                digest = info.digest;
                metaBuilder = ObjectMeta.builder(info.objectMeta);
            }
            else {
                metaBuilder = ObjectMeta.builder();
            }
        }

        public Builder bucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public Builder nuid(String nuid) {
            this.nuid = nuid;
            return this;
        }

        public Builder size(long size) {
            this.size = size;
            return this;
        }

        public Builder modified(ZonedDateTime modified) {
            this.modified = modified;
            return this;
        }

        public Builder chunks(long chunks) {
            this.chunks = chunks;
            return this;
        }

        public Builder digest(String digest) {
            this.digest = digest;
            return this;
        }

        public Builder deleted(boolean deleted) {
            this.deleted = deleted;
            return this;
        }

        public Builder meta(ObjectMeta meta) {
            metaBuilder = ObjectMeta.builder(meta);
            return this;
        }

        public ObjectInfo build() {
            return new ObjectInfo(this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ObjectInfo that = (ObjectInfo) o;
        if (size != that.size) return false;
        if (chunks != that.chunks) return false;
        if (deleted != that.deleted) return false;
        if (bucket != null ? !bucket.equals(that.bucket) : that.bucket != null) return false;
        if (nuid != null ? !nuid.equals(that.nuid) : that.nuid != null) return false;
        if (!DateTimeUtils.equals(modified, that.modified)) return false;
        if (digest != null ? !digest.equals(that.digest) : that.digest != null) return false;
        return objectMeta != null ? objectMeta.equals(that.objectMeta) : that.objectMeta == null;
    }

    @Override
    public int hashCode() {
        int result = bucket != null ? bucket.hashCode() : 0;
        result = 31 * result + (nuid != null ? nuid.hashCode() : 0);
        result = 31 * result + (int) (size ^ (size >>> 32));
        result = 31 * result + (modified != null ? modified.hashCode() : 0);
        result = 31 * result + (int) (chunks ^ (chunks >>> 32));
        result = 31 * result + (digest != null ? digest.hashCode() : 0);
        result = 31 * result + (deleted ? 1 : 0);
        result = 31 * result + (objectMeta != null ? objectMeta.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ObjectInfo{" +
            "bucket='" + bucket + '\'' +
            ", nuid='" + nuid + '\'' +
            ", size=" + size +
            ", modified=" + modified +
            ", chunks=" + chunks +
            ", digest='" + digest + '\'' +
            ", deleted=" + deleted +
            ", objectMeta=" + objectMeta +
            '}';
    }
}
