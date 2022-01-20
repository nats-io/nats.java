// Copyright 2020 The NATS Authors
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

import io.nats.client.impl.JetStreamTestBase;
import io.nats.client.support.DateTimeUtils;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;

import static io.nats.client.utils.ResourceUtils.dataAsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ObjectInfoTests extends JetStreamTestBase {

    @Test
    public void tesObjectInfo() {
        String json = dataAsString("ObjectInfo.json");
        ObjectInfo info = new ObjectInfo(json, null);
        assertObjectInfo(info);
        assertObjectInfo(new ObjectInfo(info.toJson(), null));
    }

    private void assertObjectInfo(ObjectInfo info) {
        ObjectMeta meta = info.getObjectMeta();
        ObjectMetaOptions options = meta.getObjectMetaOptions();
        ObjectLink link = options.getLink();
        assertEquals("meta-name", meta.getName());
        assertEquals("meta-desc", meta.getDescription());
        assertEquals("link-bucket", link.getBucket());
        assertEquals("link-name", link.getName());
        assertEquals(65536, options.getChunkSize());
        assertEquals("bucket", info.getBucket());
        assertEquals("123ABC456XYZ", info.getNuid());
        assertEquals(666666, info.getSize());
        assertEquals(11, info.getChunks());
        assertEquals("base64-url-encoded", info.getDigest());
        assertTrue(info.isDeleted());
        ZonedDateTime zdt = DateTimeUtils.parseDateTime("2022-01-11T22:22:22.000000000Z");
        assertEquals(zdt, info.getModified());
    }
}
