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
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalTime;
import java.util.concurrent.TimeoutException;

import static io.nats.client.impl.JsonUtils.printObject;
import static org.junit.jupiter.api.Assertions.*;

public class JetStreamManagementTests {
    private static final String STREAM1 = "test-stream-1";
    private static final String STREAM2 = "test-stream-2";
    private static final String STRM1SUB1 = "strm1sub1";
    private static final String STRM1SUB2 = "strm1sub2";
    private static final String STRM2SUB1 = "strm2sub1";
    private static final String STRM2SUB2 = "strm2sub2";

    @Test
    public void testAddStream() throws Exception {
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
    public void testUpdateStream() throws Exception {
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
    public void testUpdateNonExistentStream_isNotValid() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false, true); Connection nc = Nats.connect(ts.getURI())) {
            JetStream js = nc.jetStream();
            StreamConfiguration sc = getTestStreamConfiguration();
            assertThrows(IllegalStateException.class, () -> js.updateStream(sc));
        }
    }

    @Test
    public void testUpdateStream_cannotChangeMaxConsumers() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false, true); Connection nc = Nats.connect(ts.getURI())) {
            JetStream js = nc.jetStream();
            addTestStream(js);
            StreamConfiguration sc = getTestStreamConfigurationBuilder()
                    .maxConsumers(2)
                    .build();
            assertThrows(IllegalStateException.class, () -> js.updateStream(sc));
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
            assertThrows(IllegalStateException.class, () -> js.updateStream(sc));
        }
    }


    private StreamInfo addTestStream(JetStream js) throws TimeoutException, InterruptedException {
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

    public static void main(String[] args) {
        printObject(new ConsumerLister(TEST));
    }

    static String TEST = "{\n" +
            "  \"type\": \"io.nats.jetstream.api.v1.consumer_list_response\",\n" +
            "  \"total\": 2,\n" +
            "  \"offset\": 0,\n" +
            "  \"limit\": 256,\n" +
            "  \"consumers\": [\n" +
            "    {\n" +
            "      \"stream_name\": \"example-stream-1\",\n" +
            "      \"name\": \"GQJ3IvWo\",\n" +
            "      \"created\": \"2021-01-20T23:41:08.579594Z\",\n" +
            "      \"config\": {\n" +
            "        \"durable_name\": \"GQJ3IvWo\",\n" +
            "        \"deliver_subject\": \"strm1-deliver\",\n" +
            "        \"deliver_policy\": \"all\",\n" +
            "        \"ack_policy\": \"explicit\",\n" +
            "        \"ack_wait\": 30000000000,\n" +
            "        \"max_deliver\": -1,\n" +
            "        \"replay_policy\": \"instant\"\n" +
            "      },\n" +
            "      \"delivered\": {\n" +
            "        \"consumer_seq\": 0,\n" +
            "        \"stream_seq\": 0\n" +
            "      },\n" +
            "      \"ack_floor\": {\n" +
            "        \"consumer_seq\": 0,\n" +
            "        \"stream_seq\": 0\n" +
            "      },\n" +
            "      \"num_ack_pending\": 0,\n" +
            "      \"num_redelivered\": 0,\n" +
            "      \"num_waiting\": 0,\n" +
            "      \"num_pending\": 0\n" +
            "    },\n" +
            "    {\n" +
            "      \"stream_name\": \"example-stream-1\",\n" +
            "      \"name\": \"consumer-durable\",\n" +
            "      \"created\": \"2021-01-20T23:26:04.6445175Z\",\n" +
            "      \"config\": {\n" +
            "        \"durable_name\": \"consumer-durable\",\n" +
            "        \"deliver_subject\": \"strm1-deliver\",\n" +
            "        \"deliver_policy\": \"all\",\n" +
            "        \"ack_policy\": \"explicit\",\n" +
            "        \"ack_wait\": 30000000000,\n" +
            "        \"max_deliver\": -1,\n" +
            "        \"replay_policy\": \"instant\"\n" +
            "      },\n" +
            "      \"delivered\": {\n" +
            "        \"consumer_seq\": 0,\n" +
            "        \"stream_seq\": 0\n" +
            "      },\n" +
            "      \"ack_floor\": {\n" +
            "        \"consumer_seq\": 0,\n" +
            "        \"stream_seq\": 0\n" +
            "      },\n" +
            "      \"num_ack_pending\": 0,\n" +
            "      \"num_redelivered\": 0,\n" +
            "      \"num_waiting\": 0,\n" +
            "      \"num_pending\": 0\n" +
            "    }\n" +
            "  ]\n" +
            "}\n";
}
