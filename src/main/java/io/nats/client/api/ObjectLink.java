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
import io.nats.client.support.Validator;

import static io.nats.client.support.ApiConstants.BUCKET;
import static io.nats.client.support.ApiConstants.NAME;
import static io.nats.client.support.JsonValueUtils.readString;
import static io.nats.client.support.JsonWriteUtils.beginJson;
import static io.nats.client.support.JsonWriteUtils.endJson;

/**
 * The ObjectLink is used to embed links to other objects.
 */
public class ObjectLink implements JsonSerializable {

    private final String bucket;
    private final String objectName;

    static ObjectLink optionalInstance(JsonValue vLink) {
        return vLink == null ? null : new ObjectLink(vLink);
    }

    ObjectLink(JsonValue vLink) {
        bucket = readString(vLink, BUCKET);
        objectName = readString(vLink, NAME);
    }

    private ObjectLink(String bucket, String objectName) {
        this.bucket = Validator.validateBucketName(bucket, true);
        this.objectName = objectName;
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonWriteUtils.addField(sb, BUCKET, bucket);
        JsonWriteUtils.addField(sb, NAME, objectName);
        return endJson(sb).toString();
    }

    public String getBucket() {
        return bucket;
    }

    public String getObjectName() {
        return objectName;
    }

    public boolean isObjectLink() {
        return objectName != null;
    }

    public boolean isBucketLink() {
        return objectName == null;
    }

    public static ObjectLink bucket(String bucket) {
        return new ObjectLink(bucket, null);
    }

    public static ObjectLink object(String bucket, String objectName) {
        return new ObjectLink(bucket, objectName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ObjectLink that = (ObjectLink) o;

        if (!bucket.equals(that.bucket)) return false; // bucket never null
        return objectName != null ? objectName.equals(that.objectName) : that.objectName == null;
    }

    @Override
    public int hashCode() {
        return bucket.hashCode() * 31
            + (objectName == null ? 0 : objectName.hashCode());
    }

    @Override
    public String toString() {
        return "ObjectLink{" +
            "bucket='" + bucket + '\'' +
            ", objectName='" + objectName + '\'' +
            '}';
    }
}
