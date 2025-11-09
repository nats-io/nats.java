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
import io.nats.client.support.*;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.ZonedDateTime;
import java.util.Map;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;

/**
 * The ObjectInfo is Object Meta Information plus instance information
 */
public class ObjectInfo implements JsonSerializable {
    private final String bucket;
    private final String nuid;
    private final long size;
    private final long chunks;
    private final String digest;
    private final boolean deleted;
    private final ObjectMeta objectMeta;
    private final ZonedDateTime modified;

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
        this(mi.getData(), mi.getTime());
    }

    public ObjectInfo(Message m) {
        this(m.getData(), m.metaData().timestamp());
    }

    ObjectInfo(byte[] jsonBytes, ZonedDateTime messageTime) {
        JsonValue jv = JsonParser.parseUnchecked(jsonBytes);
        objectMeta = new ObjectMeta(jv);
        bucket = JsonValueUtils.readString(jv, BUCKET);
        nuid = JsonValueUtils.readString(jv, NUID);
        size = JsonValueUtils.readLong(jv, SIZE, 0);
        modified = DateTimeUtils.toGmt(messageTime);
        chunks = JsonValueUtils.readLong(jv, CHUNKS, 0);
        digest = JsonValueUtils.readString(jv, DIGEST);
        deleted = JsonValueUtils.readBoolean(jv, DELETED);
    }

    @Override
    @NonNull
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

    /**
     * the bucket name
     * @return the name
     */
    @NonNull
    public String getBucket() {
        return bucket;
    }

    /**
     * the bucket nuid
     * @return the nuid
     */
    @Nullable
    public String getNuid() {
        return nuid;
    }

    /**
     * The size of the object
     * @return the size in bytes
     */
    public long getSize() {
        return size;
    }

    /**
     * When the object was last modified
     * @return the last modified date
     */
    @Nullable
    public ZonedDateTime getModified() {
        return modified;
    }

    /**
     * The total number of chunks in the object
     * @return the number of chunks
     */
    public long getChunks() {
        return chunks;
    }

    /**
     * The digest string for the object
     * @return the digest
     */
    @Nullable
    public String getDigest() {
        return digest;
    }

    /**
     * Whether the object is deleted
     * @return the deleted state
     */
    public boolean isDeleted() {
        return deleted;
    }

    /**
     * The full object meta object
     * @return the ObjectMeta
     */
    @NonNull
    public ObjectMeta getObjectMeta() {
        return objectMeta;
    }

    /**
     * The object name
     * @return the object name
     */
    @Nullable
    public String getObjectName() {
        return objectMeta.getObjectName();
    }

    /**
     * The object meta description
     * @return the description text or null
     */
    @Nullable
    public String getDescription() {
        return objectMeta.getDescription();
    }

    /**
     * The object meta Headers. May be empty but will not be null. In all cases it will be unmodifiable
     * @return the headers object
     */
    @NonNull
    public Headers getHeaders() {
        return objectMeta.getHeaders();
    }

    /**
     * The object meta metadata. May be empty but will not be null. In all cases it will be unmodifiable
     * @return the map
     */
    @NonNull
    public Map<String, String> getMetaData() {
        return objectMeta.getMetadata();
    }

    /**
     * Whether the object is actually a link
     * @return true if the object is a link instead of a direct object
     */
    public boolean isLink() {
        return objectMeta.getObjectMetaOptions() != null && objectMeta.getObjectMetaOptions().getLink() != null;
    }

    /**
     * If this is a link to an object, get the ObjectLink instance, otherwise this will be null
     * @return the ObjectLink or null
     */
    @Nullable
    public ObjectLink getLink() {
        return objectMeta.getObjectMetaOptions() == null ? null : objectMeta.getObjectMetaOptions().getLink();
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

        public Builder metadata(Map<String, String> metadata) {
            metaBuilder.metadata(metadata);
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
        result = 31 * result + Long.hashCode(size);
        result = 31 * result + (modified != null ? modified.hashCode() : 0);
        result = 31 * result + Long.hashCode(chunks);
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
