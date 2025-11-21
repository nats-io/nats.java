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

import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.*;
import io.nats.client.support.DateTimeUtils;
import io.nats.client.utils.VersionUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class JetStreamMirrorAndSourcesTests extends JetStreamTestBase {

    @Test
    public void testMirrorBasics() throws Exception {
        String S1 = random();
        String S2 = random();
        String S3 = random();
        String S4 = random();
        String U1 = random();
        String U2 = random();
        String U3 = random();
        String M1 = random();

        runInLrServer((nc, jsm, js) -> {
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
            assertMirror(jsm, M1, S1, 100L, null);

            // Purge the source stream.
            jsm.purgeStream(S1);

            jsPublish(js, U2, 50);

            // Create second mirror
            sc = StreamConfiguration.builder()
                    .name(S2)
                    .storageType(StorageType.Memory)
                    .mirror(mirror)
                    .build();
            jsm.addStream(sc);

            // Check the state
            assertMirror(jsm, S2, S1, 50L, 101L);

            jsPublish(js, U3, 100);

            // third mirror checks start seq
            sc = StreamConfiguration.builder()
                    .name(S3)
                    .storageType(StorageType.Memory)
                    .mirror(Mirror.builder().sourceName(S1).startSeq(150).build())
                    .build();
            jsm.addStream(sc);

            // Check the state
            assertMirror(jsm, S3, S1, 101L, 150L);

            // third mirror checks start seq
            ZonedDateTime zdt = DateTimeUtils.fromNow(Duration.ofHours(-2));
            sc = StreamConfiguration.builder()
                    .name(S4)
                    .storageType(StorageType.Memory)
                    .mirror(Mirror.builder().sourceName(S1).startTime(zdt).build())
                    .build();
            jsm.addStream(sc);

            // Check the state
            assertMirror(jsm, S4, S1, 150L, 101L);
        });
    }

    @Test
    public void testMirrorReading() throws Exception {
        String S1 = random();
        String U1 = random();
        String U2 = random();
        String M1 = random();

        runInLrServer((nc, jsm, js) -> {
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

            assertMirror(jsm, M1, S1, 30L, null);

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

            //noinspection deprecation
            PushSubscribeOptions.bind(M1); // coverage for deprecated
            PushSubscribeOptions pso = PushSubscribeOptions.stream(M1);
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

    @Test
    public void testMirrorExceptions() throws Exception {
        runInLrServer((nc, jsm, js) -> {
            Mirror mirror = Mirror.builder().sourceName(random()).build();

            StreamConfiguration scEx = StreamConfiguration.builder()
                    .name(random())
                    .subjects(random())
                    .mirror(mirror)
                    .build();
            assertThrows(JetStreamApiException.class, () -> jsm.addStream(scEx));
        });
    }

    @Test
    public void testSourceBasics() throws Exception {
        String N1 = random();
        String N2 = random();
        String N3 = random();
        String N4 = random();
        String N5 = random();
        String N6 = random();
        String U1 = random();
        String R1 = random();
        String R2 = random();

        runInLrServer((nc, jsm, js) -> {
            // Create streams
            StreamInfo si = jsm.addStream(StreamConfiguration.builder()
                    .name(N1).storageType(StorageType.Memory).build());
            StreamConfiguration sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(N1, sc.getName());

            si = jsm.addStream(StreamConfiguration.builder()
                    .name(N2).storageType(StorageType.Memory).build());
            sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(N2, sc.getName());

            si = jsm.addStream(StreamConfiguration.builder()
                    .name(N3).storageType(StorageType.Memory).build());
            sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(N3, sc.getName());

            // Populate each one.
            jsPublish(js, N1, 10);
            jsPublish(js, N2, 15);
            jsPublish(js, N3, 25);

            sc = StreamConfiguration.builder()
                    .name(R1)
                    .storageType(StorageType.Memory)
                    .sources(Source.builder().sourceName(N1).build(),
                            Source.builder().sourceName(N2).build(),
                            Source.builder().sourceName(N3).build())
                    .build();

            jsm.addStream(sc);

            assertSource(jsm, R1, 50L, null);

            sc = StreamConfiguration.builder()
                    .name(R1)
                    .storageType(StorageType.Memory)
                    .sources(Source.builder().sourceName(N1).build(),
                            Source.builder().sourceName(N2).build(),
                            Source.builder().sourceName(N4).build())
                    .build();

            jsm.updateStream(sc);

            sc = StreamConfiguration.builder()
                    .name(N5)
                    .storageType(StorageType.Memory)
                    .subjects(N4, U1)
                    .build();
            jsm.addStream(sc);

            jsPublish(js, N4, 20);
            jsPublish(js, U1, 20);
            jsPublish(js, N4, 10);

            sc = StreamConfiguration.builder()
                    .name(R2)
                    .storageType(StorageType.Memory)
                    .sources(Source.builder().sourceName(N5).startSeq(26).build())
                    .build();
            jsm.addStream(sc);
            assertSource(jsm, R2, 25L, null);

            MessageInfo info = jsm.getMessage(R2, 1);
            assertStreamSource(info, N5, 26);

            sc = StreamConfiguration.builder()
                    .name(N6)
                    .storageType(StorageType.Memory)
                    .sources(Source.builder().sourceName(N5).startSeq(11).filterSubject(N4).build())
                    .build();
            jsm.addStream(sc);
            assertSource(jsm, N6, 20L, null);

            info = jsm.getMessage(N6, 1);
            assertStreamSource(info, N5, 11);
        });
    }

    @Test
    @Disabled("This used to work.")
    public void testSourceAndTransformsRoundTrips() throws Exception {
        runInLrServer(VersionUtils::atLeast2_10, (nc, jsm, js) -> {
            StreamConfiguration scSource = StreamConfigurationTests.getStreamConfigurationFromJson(
                "StreamConfigurationSourcedSubjectTransform.json");

            StreamInfo si = jsm.addStream(scSource);
            assertNull(scSource.getMirror());
            assertNull(si.getMirrorInfo());

            assertNotNull(scSource.getSources());
            assertNotNull(si.getSourceInfos());
            Source source = scSource.getSources().get(0);
            SourceInfo info = si.getSourceInfos().get(0);
            assertNotNull(info);
            assertNotNull(info.getSubjectTransforms());
            assertEquals(1, info.getSubjectTransforms().size());

            assertEquals(source.getName(), info.getName());
            assertNotNull(source.getSubjectTransforms());
            assertEquals(1, source.getSubjectTransforms().size());

            SubjectTransform st = source.getSubjectTransforms().get(0);
            SubjectTransform infoSt = info.getSubjectTransforms().get(0);
            assertEquals(st.getSource(), infoSt.getSource());
            assertEquals(st.getDestination(), infoSt.getDestination());

            source = scSource.getSources().get(1);
            info = si.getSourceInfos().get(1);
            assertNotNull(scSource.getSources());
            assertNotNull(si.getSourceInfos());
            assertEquals(source.getName(), info.getName());
            assertNotNull(info.getSubjectTransforms());
            assertEquals(1, info.getSubjectTransforms().size());
            assertNotNull(source.getSubjectTransforms());
            st = source.getSubjectTransforms().get(0);
            infoSt = info.getSubjectTransforms().get(0);
            assertEquals(st.getSource(), infoSt.getSource());
            assertEquals(st.getDestination(), infoSt.getDestination());
        });
    }

    @Test
    public void testMirror() throws Exception {
        runInLrServer(VersionUtils::atLeast2_10, (nc, jsm, js) -> {
            StreamConfiguration scMirror = StreamConfigurationTests.getStreamConfigurationFromJson(
                "StreamConfigurationMirrorSubjectTransform.json");

            StreamInfo si = jsm.addStream(scMirror);
            Mirror m = scMirror.getMirror();
            MirrorInfo mi = si.getMirrorInfo();
            assertNotNull(m);
            assertNotNull(mi);
            assertEquals(m.getName(), mi.getName());
            assertEquals(m.getSubjectTransforms(), mi.getSubjectTransforms());
            assertNotNull(scMirror.getSources());
            assertTrue(scMirror.getSources().isEmpty());
            assertNull(si.getSourceInfos());
        });
    }
}
