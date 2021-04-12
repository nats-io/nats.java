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

package io.nats.client.impl;

import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.client.support.DateTimeUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class JetStreamMirrorTests extends JetStreamTestBase {
    static final String S1 = stream(1);
    static final String S2 = stream(2);
    static final String S3 = stream(3);
    static final String S4 = stream(4);
    static final String S5 = stream(5);
    static final String S99 = stream(99);
    static final String U1 = subject(1);
    static final String U2 = subject(2);
    static final String U3 = subject(3);
    static final String M1 = mirror(1);
    static final String R1 = source(1);
    static final String R2 = source(2);

    @Test
    public void testMirrorBasics() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            Mirror mirror = Mirror.builder().sourceName(S1).build();

            // Create source stream
            StreamConfiguration sc = StreamConfiguration.builder()
                    .name(S1)
                    .storageType(StorageType.Memory)
                    .subjects(U1, U2, U3)
                    .build();
            StreamInfo si = jsm.addStream(sc);
            sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(S1, sc.getName());

            // Now create our mirror stream.
            sc = StreamConfiguration.builder()
                    .name(M1)
                    .storageType(StorageType.Memory)
                    .mirror(mirror)
                    .build();
            jsm.addStream(sc);
            assertMirror(jsm, M1, S1, null, null);

            // Send 100 messages.
            jsPublish(js, U2, 100);

            // Check the state
            assertMirror(jsm, M1, S1, 100, null);

            // Purge the source stream.
            jsm.purgeStream(S1);

            jsPublish(js, U2, 50);

            // Create second mirror
            sc = StreamConfiguration.builder()
                    .name(mirror(2))
                    .storageType(StorageType.Memory)
                    .mirror(mirror)
                    .build();
            jsm.addStream(sc);

            // Check the state
            assertMirror(jsm, mirror(2), S1, 50, 101);

            jsPublish(js, U3, 100);

            // third mirror checks start seq
            sc = StreamConfiguration.builder()
                    .name(mirror(3))
                    .storageType(StorageType.Memory)
                    .mirror(Mirror.builder().sourceName(S1).startSeq(150).build())
                    .build();
            jsm.addStream(sc);

            // Check the state
            assertMirror(jsm, mirror(3), S1, 101, 150);

            // third mirror checks start seq
            ZonedDateTime zdt = DateTimeUtils.fromNow(Duration.ofHours(-2));
            sc = StreamConfiguration.builder()
                    .name(mirror(4))
                    .storageType(StorageType.Memory)
                    .mirror(Mirror.builder().sourceName(S1).startTime(zdt).build())
                    .build();
            jsm.addStream(sc);

            // Check the state
            assertMirror(jsm, mirror(4), S1, 150, 101);
        });
    }

    @Test
    public void testMirrorReading() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            // Create source stream
            StreamConfiguration sc = StreamConfiguration.builder()
                    .name(S1)
                    .storageType(StorageType.Memory)
                    .subjects(U1, U2)
                    .build();
            StreamInfo si = jsm.addStream(sc);
            sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(S1, sc.getName());

            Mirror mirror = Mirror.builder().sourceName(S1).build();

            // Now create our mirror stream.
            sc = StreamConfiguration.builder()
                    .name(M1)
                    .storageType(StorageType.Memory)
                    .mirror(mirror)
                    .build();
            jsm.addStream(sc);
            assertMirror(jsm, M1, S1, null, null);

            // Send messages.
            jsPublish(js, U1, 10);
            jsPublish(js, U2, 20);

            assertMirror(jsm, M1, S1, 30, null);

            JetStreamSubscription sub = js.subscribe(U1);
            List<Message> list = readMessagesAck(sub);
            assertEquals(10, list.size());
            for (Message m : list) {
                assertEquals(S1, m.metaData().getStream());
            }

            sub = js.subscribe(U2);
            list = readMessagesAck(sub);
            assertEquals(20, list.size());
            for (Message m : list) {
                assertEquals(S1, m.metaData().getStream());
            }

            PushSubscribeOptions pso = PushSubscribeOptions.source(M1);
            sub = js.subscribe(U1, pso);
            list = readMessagesAck(sub);
            assertEquals(10, list.size());
            for (Message m : list) {
                assertEquals(M1, m.metaData().getStream());
            }

            sub = js.subscribe(U2, pso);
            list = readMessagesAck(sub);
            assertEquals(20, list.size());
            for (Message m : list) {
                assertEquals(M1, m.metaData().getStream());
            }
        });
    }

    private void assertMirror(JetStreamManagement jsm, String stream, String mirroring, Number msgCount, Number firstSeq)
            throws IOException, JetStreamApiException {
        sleep(1000);
        StreamInfo si = jsm.getStreamInfo(stream);

        MirrorInfo msi = si.getMirrorInfo();
        assertNotNull(msi);
        assertEquals(mirroring, msi.getName());

        assertConfig(stream, msgCount, firstSeq, si);
    }

    private void assertConfig(String stream, Number msgCount, Number firstSeq, StreamInfo si) {
        StreamConfiguration sc = si.getConfiguration();
        assertNotNull(sc);
        assertEquals(stream, sc.getName());

        StreamState ss = si.getStreamState();
        if (msgCount != null) {
            assertEquals(msgCount.longValue(), ss.getMsgCount());
        }
        if (firstSeq != null) {
            assertEquals(firstSeq.longValue(), ss.getFirstSequence());
        }
    }

    @Test
    public void testMirrorExceptions() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            Mirror mirror = Mirror.builder().sourceName(STREAM).build();

            StreamConfiguration scEx = StreamConfiguration.builder()
                    .name(mirror(99))
                    .subjects(subject(1))
                    .mirror(mirror)
                    .build();
            assertThrows(JetStreamApiException.class, () -> jsm.addStream(scEx));
        });
    }

    @Test
    public void testSourceBasics() throws Exception {
        runInJsServer(nc -> {

            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            // Create streams
            StreamInfo si = jsm.addStream(StreamConfiguration.builder()
                    .name(S1).storageType(StorageType.Memory).build());
            StreamConfiguration sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(S1, sc.getName());

            si = jsm.addStream(StreamConfiguration.builder()
                    .name(S2).storageType(StorageType.Memory).build());
            sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(S2, sc.getName());

            si = jsm.addStream(StreamConfiguration.builder()
                    .name(S3).storageType(StorageType.Memory).build());
            sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(S3, sc.getName());

            // Populate each one.
            jsPublish(js, S1, 10);
            jsPublish(js, S2, 15);
            jsPublish(js, S3, 25);

            sc = StreamConfiguration.builder()
                    .name(R1)
                    .storageType(StorageType.Memory)
                    .sources(Source.builder().sourceName(S1).build(),
                            Source.builder().sourceName(S2).build(),
                            Source.builder().sourceName(S3).build())
                    .build();

            jsm.addStream(sc);

            assertSource(jsm, R1, 50, null);

            sc = StreamConfiguration.builder()
                    .name(R1)
                    .storageType(StorageType.Memory)
                    .sources(Source.builder().sourceName(S1).build(),
                            Source.builder().sourceName(S2).build(),
                            Source.builder().sourceName(S4).build())
                    .build();

            jsm.updateStream(sc);

            sc = StreamConfiguration.builder()
                    .name(S99)
                    .storageType(StorageType.Memory)
                    .subjects(S4, S5)
                    .build();
            jsm.addStream(sc);

            jsPublish(js, S4, 20);
            jsPublish(js, S5, 20);
            jsPublish(js, S4, 10);

            sc = StreamConfiguration.builder()
                    .name(R2)
                    .storageType(StorageType.Memory)
                    .sources(Source.builder().sourceName(S99).startSeq(26).build())
                    .build();
            jsm.addStream(sc);
            assertSource(jsm, R2, 25, null);

            MessageInfo info = jsm.getMessage(R2, 1);
            String hval = info.getHeaders().get("Nats-Stream-Source").get(0);
            // $JS.ACK.stream-99.sS0mKw3k.1.26.1.1616619886415151100.24
            String[] parts = hval.split("\\.");
            assertEquals(S99, parts[2]);
            assertEquals("26", parts[5]);

            sc = StreamConfiguration.builder()
                    .name(source(3))
                    .storageType(StorageType.Memory)
                    .sources(Source.builder().sourceName(S99).startSeq(11).filterSubject(S4).build())
                    .build();
            jsm.addStream(sc);
            assertSource(jsm, source(3), 20, null);

            info = jsm.getMessage(source(3), 1);
            hval = info.getHeaders().get("Nats-Stream-Source").get(0);
            parts = hval.split("\\.");
            assertEquals(S99, parts[2]);
            assertEquals("11", parts[5]);
        });
    }

    private void assertSource(JetStreamManagement jsm, String stream, Number msgCount, Number firstSeq)
            throws IOException, JetStreamApiException {
        sleep(1000);
        StreamInfo si = jsm.getStreamInfo(stream);

        assertConfig(stream, msgCount, firstSeq, si);
    }
}
