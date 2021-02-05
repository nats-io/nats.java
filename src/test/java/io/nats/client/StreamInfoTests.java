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

import io.nats.client.impl.JsonUtils;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static io.nats.client.utils.ResourceUtils.dataAsString;
import static org.junit.jupiter.api.Assertions.*;

public class StreamInfoTests {

    @Test
    public void testStreamInfo() {
        String json = dataAsString("StreamInfo.json");
        StreamInfo si = new StreamInfo(json);
        long expected = JsonUtils.parseDateTime("2021-01-25T20:09:10.6225191Z").toEpochSecond();
        assertEquals(expected, si.getCreateTime().toEpochSecond());

        StreamConfiguration sc = si.getConfiguration();
        assertEquals("streamName", sc.getName());
        assertEquals(2, sc.getSubjects().length);
        assertEquals("sub0", sc.getSubjects()[0]);
        assertEquals("sub1", sc.getSubjects()[1]);

        assertEquals(StreamConfiguration.RetentionPolicy.Limits, sc.getRetentionPolicy());
        assertEquals(StreamConfiguration.DiscardPolicy.Old, sc.getDiscardPolicy());
        assertEquals(StreamConfiguration.StorageType.Memory, sc.getStorageType());

        assertNotNull(si.getConfiguration());
        assertNotNull(si.getStreamState());
        assertEquals(1, sc.getMaxConsumers());
        assertEquals(2, sc.getMaxMsgs());
        assertEquals(3, sc.getMaxBytes());
        assertEquals(4, sc.getMaxMsgSize());
        assertEquals(5, sc.getReplicas());

        assertEquals(Duration.ofSeconds(100), sc.getMaxAge());
        assertEquals(Duration.ofSeconds(120), sc.getDuplicateWindow());

        StreamInfo.StreamState ss = si.getStreamState();
        assertEquals(11, ss.getMsgCount());
        assertEquals(12, ss.getByteCount());
        assertEquals(13, ss.getFirstSequence());
        assertEquals(14, ss.getLastSequence());
        assertEquals(15, ss.getConsumerCount());

        assertEquals(JsonUtils.parseDateTime("0001-01-01T00:00:00Z"), ss.getFirstTime());
        assertEquals(JsonUtils.parseDateTime("0001-01-01T00:00:00Z"), ss.getLastTime());

        si = new StreamInfo("{}");
        assertNull(si.getCreateTime());
        assertNotNull(si.getStreamState());
        assertNotNull(si.getConfiguration());
    }
}