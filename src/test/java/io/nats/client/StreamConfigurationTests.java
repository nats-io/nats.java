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
import io.nats.client.impl.JetStreamTestBase;
import io.nats.client.utils.ResourceUtils;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.*;

public class StreamConfigurationTests extends JetStreamTestBase {

    @Test
    public void testBuilder() {
        // manual construction
        StreamConfiguration sc = StreamConfiguration.builder()
                .discardPolicy(DiscardPolicy.Old)
                .duplicateWindow(Duration.ofSeconds(99))
                .maxAge(Duration.ofDays(42))
                .maxBytes(1024*1024*420)
                .maxConsumers(512)
                .maxMessages(64)
                .maxMsgSize(1024*1024)
                .name("stream-name")
                .noAck(true)
                .replicas(5)
                .storageType(StorageType.Memory)
                .retentionPolicy(RetentionPolicy.Interest)
                .subjects("foo", "bar")
                .template("tpl").build();
        validateBuilder(sc);

        // copy construction
        validateBuilder(StreamConfiguration.builder(sc).build());
    }

    private void validateBuilder(StreamConfiguration c) {
        assertEquals(DiscardPolicy.Old, c.getDiscardPolicy());
        assertEquals(Duration.ofSeconds(99), c.getDuplicateWindow());
        assertEquals(Duration.ofDays(42), c.getMaxAge());
        assertEquals(1024 * 1024 * 420, c.getMaxBytes());
        assertEquals(512, c.getMaxConsumers());
        assertEquals(64, c.getMaxMsgs());
        assertEquals(1024 * 1024, c.getMaxMsgSize());
        assertEquals("stream-name", c.getName());
        assertTrue(c.getNoAck());
        assertEquals(5, c.getReplicas());
        assertEquals(StorageType.Memory, c.getStorageType());
        assertEquals(RetentionPolicy.Interest, c.getRetentionPolicy());
        assertNotNull(c.getSubjects());
        assertEquals(2, c.getSubjects().size());
        assertEquals("foo", c.getSubjects().get(0));
        assertEquals("bar", c.getSubjects().get(1));
        assertEquals("tpl", c.getTemplateOwner());
    }

    @Test
    public void testSubjects() {
        StreamConfiguration.Builder builder = StreamConfiguration.builder();

        // subjects(...) replaces
        builder.subjects(subject(0));
        assertSubjects(builder.build(), 0);

        // subjects(...) replaces
        builder.subjects();
        assertSubjects(builder.build());

        // subjects(...) replaces
        builder.subjects(subject(1));
        assertSubjects(builder.build(), 1);

        // subjects(...) replaces
        builder.subjects((String)null);
        assertSubjects(builder.build());

        // subjects(...) replaces
        builder.subjects(subject(2), subject(3));
        assertSubjects(builder.build(), 2, 3);

        // subjects(...) replaces
        builder.subjects(subject(101), null, subject(102));
        assertSubjects(builder.build(), 101, 102);

        // subjects(...) replaces
        builder.subjects(Arrays.asList(subject(4), subject(5)));
        assertSubjects(builder.build(), 4, 5);

        // addSubjects(...) adds unique
        builder.addSubjects(subject(5), subject(6));
        assertSubjects(builder.build(), 4, 5, 6);

        // addSubjects(...) adds unique
        builder.addSubjects(Arrays.asList(subject(6), subject(7), subject(8)));
        assertSubjects(builder.build(), 4, 5, 6, 7, 8);

        // addSubjects(...) null check
        builder.addSubjects((String[]) null);
        assertSubjects(builder.build(), 4, 5, 6, 7, 8);

        // addSubjects(...) null check
        builder.addSubjects((Collection<String>) null);
        assertSubjects(builder.build(), 4, 5, 6, 7, 8);
    }

    private void assertSubjects(StreamConfiguration sc, int... subIds) {
        assertEquals(subIds.length, sc.getSubjects().size());
        for (int subId : subIds) {
            assertTrue(sc.getSubjects().contains(subject(subId)));
        }
    }

    @Test
    public void testRetentionPolicy() {
        StreamConfiguration.Builder builder = StreamConfiguration.builder();
        assertEquals(RetentionPolicy.Limits, builder.build().getRetentionPolicy());

        builder.retentionPolicy(RetentionPolicy.Interest);
        assertEquals(RetentionPolicy.Interest, builder.build().getRetentionPolicy());

        builder.retentionPolicy(RetentionPolicy.WorkQueue);
        assertEquals(RetentionPolicy.WorkQueue, builder.build().getRetentionPolicy());

        builder.retentionPolicy(null);
        assertEquals(RetentionPolicy.Limits, builder.build().getRetentionPolicy());
    }


    @Test
    public void testStorageType() {
        StreamConfiguration.Builder builder = StreamConfiguration.builder();
        assertEquals(StorageType.File, builder.build().getStorageType());

        builder.storageType(StorageType.Memory);
        assertEquals(StorageType.Memory, builder.build().getStorageType());

        builder.storageType(null);
        assertEquals(StorageType.File, builder.build().getStorageType());
    }

    @Test
    public void testDiscardPolicy() {
        StreamConfiguration.Builder builder = StreamConfiguration.builder();
        assertEquals(DiscardPolicy.Old, builder.build().getDiscardPolicy());

        builder.discardPolicy(DiscardPolicy.New);
        assertEquals(DiscardPolicy.New, builder.build().getDiscardPolicy());

        builder.discardPolicy(null);
        assertEquals(DiscardPolicy.Old, builder.build().getDiscardPolicy());
    }

    @Test
    public void testJSONParsing() {
        String configJSON = ResourceUtils.dataAsString("StreamConfiguration.json");
        // spot check a configuration with spaces, \n, etc.
        StreamConfiguration c = StreamConfiguration.fromJson(configJSON);
        assertEquals("sname", c.getName());
        assertEquals(2, c.getSubjects().size());
        assertEquals("foo", c.getSubjects().get(0));
        assertEquals("bar", c.getSubjects().get(1));
        assertEquals("owner", c.getTemplateOwner());
        assertEquals(StorageType.Memory, c.getStorageType());
    }

    @Test
    public void testToString() {
        // COVERAGE
        String json = ResourceUtils.dataAsString("StreamConfiguration.json");
        assertNotNull(StreamConfiguration.fromJson(json).toString());
    }
}
