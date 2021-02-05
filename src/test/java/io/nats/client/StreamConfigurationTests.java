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

package io.nats.client;

import io.nats.client.StreamConfiguration.DiscardPolicy;
import io.nats.client.StreamConfiguration.RetentionPolicy;
import io.nats.client.StreamConfiguration.StorageType;
import io.nats.client.utils.ResourceUtils;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

public class StreamConfigurationTests {
    @Test
    public void testBuilder() {
        StreamConfiguration c = StreamConfiguration.builder()
            .discardPolicy(DiscardPolicy.Old)
            .duplicateWindow(Duration.ofSeconds(99))
            .maxAge(Duration.ofDays(42))
            .maxBytes(1024*1024*420)
            .maxConsumers(512)
            .maxMsgSize(1024*1024)
            .name("stream-name")
            .noAck(true)
            .replicas(42)
            .storageType(StorageType.Memory)
            .retentionPolicy(RetentionPolicy.Interest)
            .subjects("foo", "bar")
            .template("fake").build();

        assertEquals(DiscardPolicy.Old, c.getDiscardPolicy());
        assertEquals(Duration.ofSeconds(99), c.getDuplicateWindow());
        assertEquals(Duration.ofDays(42), c.getMaxAge());
        assertEquals(1024*1024*420, c.getMaxBytes());
        assertEquals(512, c.getMaxConsumers());
        assertEquals(1024*1024, c.getMaxMsgSize());
        assertEquals("stream-name", c.getName());
        assertTrue(c.getNoAck());
        assertEquals(42, c.getReplicas());
        assertEquals(StorageType.Memory, c.getStorageType());
        assertEquals(RetentionPolicy.Interest, c.getRetentionPolicy());
        assertNotNull(c.getSubjects());
        assertEquals(2, c.getSubjects().length);
        assertEquals("foo", c.getSubjects()[0]);
        assertEquals("bar", c.getSubjects()[1]);
        assertEquals("fake", c.getTemplate());

        String json = c.toJSON();
        assertTrue(json.length() > 0);

        c = new StreamConfiguration(json);
        assertEquals(DiscardPolicy.Old, c.getDiscardPolicy());
        assertEquals(Duration.ofSeconds(99), c.getDuplicateWindow());
        assertEquals(Duration.ofDays(42), c.getMaxAge());
        assertEquals(1024*1024*420, c.getMaxBytes());
        assertEquals(512, c.getMaxConsumers());
        assertEquals(1024*1024, c.getMaxMsgSize());
        assertEquals("stream-name", c.getName());
        assertTrue(c.getNoAck());
        assertEquals(42, c.getReplicas());
        assertEquals(StorageType.Memory, c.getStorageType());
        assertEquals(RetentionPolicy.Interest, c.getRetentionPolicy());
        assertNotNull(c.getSubjects());
        assertEquals(2, c.getSubjects().length);
        assertEquals("foo", c.getSubjects()[0]);
        assertEquals("bar", c.getSubjects()[1]);
        assertEquals("fake", c.getTemplate());
    }

    @Test
    public void testJSONParsing() {
        String configJSON = ResourceUtils.dataAsString("StreamConfiguration.json");
        // spot check a configuration with spaces, \n, etc.
        StreamConfiguration c = new StreamConfiguration(configJSON);
        assertEquals("sname", c.getName());
        assertEquals(2, c.getSubjects().length);
        assertEquals("foo", c.getSubjects()[0]);
        assertEquals("bar", c.getSubjects()[1]);
        assertNull(c.getTemplate());
        assertEquals(StorageType.Memory, c.getStorageType());
    }

}