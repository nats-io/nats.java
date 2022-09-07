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
import io.nats.client.impl.Headers;
import io.nats.client.support.DateTimeUtils;
import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonUtils;
import io.nats.client.support.Validator;

import java.time.ZonedDateTime;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.*;

/**
 * The ObjectInfo is Object Meta Information plus instance information
 * OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
 */
public class ObjectInfo implements JsonSerializable {
    private final String bucket;
    private final String nuid;
    private final long size;
    private final long chunks;
    private final String digest;
    private final boolean deleted;
    private final ObjectMeta objectMeta;

    private ZonedDateTime modified;

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
        this(new String(mi.getData()), mi.getTime());
    }

    public ObjectInfo(Message m) {
        this(new String(m.getData()), m.metaData().timestamp());
    }

    ObjectInfo(String json, ZonedDateTime messageTime) {
        objectMeta = new ObjectMeta(json);

        // options has already been read by ObjectMeta
        // We do this (remove the options field) b/c
        // options has a field called link, link has a field called bucket,
        // and it will mess up the regex reading of the Object info's bucket field
        json = removeObject(json, OPTIONS);

        bucket = JsonUtils.readString(json, BUCKET_RE);
        nuid = JsonUtils.readString(json, NUID_RE);
        size = JsonUtils.readLong(json, SIZE_RE, 0);
        modified = DateTimeUtils.toGmt(messageTime);
        chunks = JsonUtils.readLong(json, CHUNKS_RE, 0);
        digest = JsonUtils.readString(json, DIGEST_RE);
        deleted = JsonUtils.readBoolean(json, DELETED_RE);
    }

    @Override
    public String toJson() {
        // never write MTIME (modified)
        StringBuilder sb = beginJson();
        objectMeta.embedJson(sb); // the go code embeds the objectMeta's fields instead of as a child object.
        JsonUtils.addField(sb, BUCKET, bucket);
        JsonUtils.addField(sb, NUID, nuid);
        JsonUtils.addField(sb, SIZE, size);
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

    public String getObjectName() {
        return objectMeta.getObjectName();
    }

    public String getDescription() {
        return objectMeta.getDescription();
    }

    public Headers getHeaders() {
        return objectMeta.getHeaders();
    }

    public boolean isLink() {
        return objectMeta.getObjectMetaOptions().getLink() != null;
    }

    public ObjectLink getLink() {
        return objectMeta.getObjectMetaOptions().getLink();
    }

    public static Builder builder(String bucket, String objectName) {
        return new Builder(bucket, objectName);
    }

    public static Builder builder(String bucket, ObjectMeta meta) {
        return new Builder(bucket, meta);
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

        public Builder(String bucket, String objectName) {
            metaBuilder = ObjectMeta.builder(objectName);
            bucket(bucket);
        }

        public Builder(String bucket, ObjectMeta meta) {
            metaBuilder = ObjectMeta.builder(meta);
            bucket(bucket);
        }

        public Builder(ObjectInfo info) {
            bucket = info.bucket;
            nuid = info.nuid;
            size = info.size;
            modified = info.modified;
            chunks = info.chunks;
            digest = info.digest;
            deleted = info.deleted;
            metaBuilder = ObjectMeta.builder(info.objectMeta);
        }

        public Builder objectName(String name) {
            metaBuilder.objectName(name);
            return this;
        }

        public Builder bucket(String bucket) {
            this.bucket = Validator.validateBucketName(bucket, true);
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

        public Builder description(String description) {
            metaBuilder.description(description);
            return this;
        }

        public Builder headers(Headers headers) {
            metaBuilder.headers(headers);
            return this;
        }

        public Builder options(ObjectMetaOptions objectMetaOptions) {
            metaBuilder.options(objectMetaOptions);
            return this;
        }

        public Builder chunkSize(int chunkSize) {
            metaBuilder.chunkSize(chunkSize);
            return this;
        }

        public Builder link(ObjectLink link) {
            metaBuilder.link(link);
            return this;
        }

        public Builder bucketLink(String bucket) {
            metaBuilder.link(ObjectLink.bucket(bucket));
            return this;
        }

        public Builder objectLink(String bucket, String objectName) {
            metaBuilder.link(ObjectLink.object(bucket, objectName));
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

        ObjectInfo info = (ObjectInfo) o;

        if (size != info.size) return false;
        if (chunks != info.chunks) return false;
        if (deleted != info.deleted) return false;
        if (!bucket.equals(info.bucket)) return false; // bucket never null
        if (nuid != null ? !nuid.equals(info.nuid) : info.nuid != null) return false;
        if (modified != null ? !modified.equals(info.modified) : info.modified != null) return false;
        if (digest != null ? !digest.equals(info.digest) : info.digest != null) return false;
        return objectMeta.equals(info.objectMeta);
    }

    @Override
    public int hashCode() {
        int result = bucket.hashCode(); // bucket never null
        result = 31 * result + (nuid != null ? nuid.hashCode() : 0);
        result = 31 * result + (int) (size ^ (size >>> 32));
        result = 31 * result + (modified != null ? modified.hashCode() : 0);
        result = 31 * result + (int) (chunks ^ (chunks >>> 32));
        result = 31 * result + (digest != null ? digest.hashCode() : 0);
        result = 31 * result + (deleted ? 1 : 0);
        result = 31 * result + objectMeta.hashCode();
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
