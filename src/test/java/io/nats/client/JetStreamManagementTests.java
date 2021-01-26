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
import io.nats.client.impl.JetStreamApiException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalTime;

import static org.junit.jupiter.api.Assertions.*;

public class JetStreamManagementTests {
    private static final String STREAM1 = "test-stream-1";
    private static final String STREAM2 = "test-stream-2";
    private static final String STRM1SUB1 = "strm1sub1";
    private static final String STRM1SUB2 = "strm1sub2";
    private static final String STRM2SUB1 = "strm2sub1";
    private static final String STRM2SUB2 = "strm2sub2";

    @Test
    public void addStream() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false, true); Connection nc = Nats.connect(ts.getURI())) {
            LocalTime now = LocalTime.now();

            JetStream js = nc.jetStream();
            StreamInfo si = addTestStream(js);
            assertTrue(now.isBefore(si.getCreateTime().toLocalTime()));

            StreamConfiguration sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(STREAM1, sc.getName());
            assertNotNull(sc.getSubjects());
            assertEquals(2, sc.getSubjects().length);
            assertEquals(STRM1SUB1, sc.getSubjects()[0]);
            assertEquals(STRM1SUB2, sc.getSubjects()[1]);
            assertEquals(RetentionPolicy.Limits, sc.getRetentionPolicy());
            assertEquals(-1, sc.getMaxConsumers());
            assertEquals(-1, sc.getMaxBytes());
            assertEquals(-1, sc.getMaxMsgSize());
            assertEquals(Duration.ZERO, sc.getMaxAge());
            assertEquals(StorageType.Memory, sc.getStorageType());
            assertEquals(DiscardPolicy.Old, sc.getDiscardPolicy());
            assertEquals(1, sc.getReplicas());
            assertFalse(sc.getNoAck());
            assertEquals(Duration.ofMinutes(2), sc.getDuplicateWindow());
            assertNull(sc.getTemplate());

            StreamInfo.StreamState state = si.getStreamState();
            assertNotNull(state);
            assertEquals(0, state.getMsgCount());
            assertEquals(0, state.getByteCount());
            assertEquals(0, state.getFirstSequence());
            assertEquals(0, state.getMsgCount());
            assertEquals(0, state.getLastSequence());
            assertEquals(0, state.getConsumerCount());
        }
    }

    @Test
    public void updateStream() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false, true); Connection nc = Nats.connect(ts.getURI())) {
            JetStream js = nc.jetStream();
            addTestStream(js);

            StreamConfiguration sc = StreamConfiguration.builder()
                    .name(STREAM1)
                    .storageType(StorageType.Memory)
                    .subjects(STRM2SUB1, STRM2SUB2)
                    .maxBytes(43)
                    .maxMsgSize(44)
                    .maxAge(Duration.ofDays(100))
                    .discardPolicy(DiscardPolicy.New)
                    .noAck(true)
                    .duplicateWindow(Duration.ofMinutes(3))
                    .build();
            StreamInfo si = js.updateStream(sc);
            assertNotNull(si);

            sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(STREAM1, sc.getName());
            assertNotNull(sc.getSubjects());
            assertEquals(2, sc.getSubjects().length);
            assertEquals(STRM2SUB1, sc.getSubjects()[0]);
            assertEquals(STRM2SUB2, sc.getSubjects()[1]);
            assertEquals(43, sc.getMaxBytes());
            assertEquals(44, sc.getMaxMsgSize());
            assertEquals(Duration.ofDays(100), sc.getMaxAge());
            assertEquals(StorageType.Memory, sc.getStorageType());
            assertEquals(DiscardPolicy.New, sc.getDiscardPolicy());
            assertEquals(1, sc.getReplicas());
            assertTrue(sc.getNoAck());
            assertEquals(Duration.ofMinutes(3), sc.getDuplicateWindow());
            assertNull(sc.getTemplate());
        }
    }

    @Test
    public void addOrUpdateStream_nullConfiguration_isNotValid() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false, true); Connection nc = Nats.connect(ts.getURI())) {
            JetStream js = nc.jetStream();
            assertThrows(IllegalArgumentException.class, () -> js.updateStream(null));
        }
    }

    @Test
    public void updateStream_cannotUpdate_nonExistentStream() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false, true); Connection nc = Nats.connect(ts.getURI())) {
            JetStream js = nc.jetStream();
            StreamConfiguration sc = getTestStreamConfiguration();
            assertThrows(JetStreamApiException.class, () -> js.updateStream(sc));
        }
    }

    @Test
    public void updateStream_cannotChangeMaxConsumers() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false, true); Connection nc = Nats.connect(ts.getURI())) {
            JetStream js = nc.jetStream();
            addTestStream(js);
            StreamConfiguration sc = getTestStreamConfigurationBuilder()
                    .maxConsumers(2)
                    .build();
            assertThrows(JetStreamApiException.class, () -> js.updateStream(sc));
        }
    }

    @Test
    public void testUpdateStream_cannotChangeRetentionPolicy() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false, true); Connection nc = Nats.connect(ts.getURI())) {
            JetStream js = nc.jetStream();
            addTestStream(js);
            StreamConfiguration sc = getTestStreamConfigurationBuilder()
                    .retentionPolicy(RetentionPolicy.Interest)
                    .build();
            assertThrows(JetStreamApiException.class, () -> js.updateStream(sc));
        }
    }


    private StreamInfo addTestStream(JetStream js) throws IOException, JetStreamApiException {
        StreamInfo si = js.addStream(getTestStreamConfiguration());
        assertNotNull(si);

        return si;
    }

    private StreamConfiguration getTestStreamConfiguration() {
        return getTestStreamConfigurationBuilder().build();
    }

    private StreamConfiguration.Builder getTestStreamConfigurationBuilder() {
        return StreamConfiguration.builder()
                .name(STREAM1)
                .storageType(StorageType.Memory)
                .subjects(STRM1SUB1, STRM1SUB2);
    }
}
