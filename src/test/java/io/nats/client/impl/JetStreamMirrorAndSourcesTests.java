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

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class JetStreamMirrorAndSourcesTests extends JetStreamTestBase {

    @Test
    public void testMirrorBasics() throws Exception {
        String S1 = stream();
        String U1 = subject();
        String U2 = subject();
        String U3 = subject();
        String M1 = mirror();

        jsServer.run(nc -> {
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
            assertMirror(jsm, M1, S1, 100L, null);

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
            assertMirror(jsm, mirror(2), S1, 50L, 101L);

            jsPublish(js, U3, 100);

            // third mirror checks start seq
            sc = StreamConfiguration.builder()
                    .name(mirror(3))
                    .storageType(StorageType.Memory)
                    .mirror(Mirror.builder().sourceName(S1).startSeq(150).build())
                    .build();
            jsm.addStream(sc);

            // Check the state
            assertMirror(jsm, mirror(3), S1, 101L, 150L);

            // third mirror checks start seq
            ZonedDateTime zdt = DateTimeUtils.fromNow(Duration.ofHours(-2));
            sc = StreamConfiguration.builder()
                    .name(mirror(4))
                    .storageType(StorageType.Memory)
                    .mirror(Mirror.builder().sourceName(S1).startTime(zdt).build())
                    .build();
            jsm.addStream(sc);

            // Check the state
            assertMirror(jsm, mirror(4), S1, 150L, 101L);
        });
    }

    @Test
    public void testMirrorReading() throws Exception {
        String S1 = stream();
        String U1 = subject();
        String U2 = subject();
        String M1 = mirror();

        jsServer.run(nc -> {
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
        jsServer.run(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            Mirror mirror = Mirror.builder().sourceName(STREAM).build();

            StreamConfiguration scEx = StreamConfiguration.builder()
                    .name(mirror())
                    .subjects(subject())
                    .mirror(mirror)
                    .build();
            assertThrows(JetStreamApiException.class, () -> jsm.addStream(scEx));
        });
    }

    @Test
    public void testSourceBasics() throws Exception {
        String S1 = stream();
        String S2 = stream();
        String S3 = stream();
        String S4 = stream();
        String S5 = stream();
        String S99 = stream();
        String R1 = source();
        String R2 = source();

        jsServer.run(nc -> {
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

            assertSource(jsm, R1, 50L, null);

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
            assertSource(jsm, R2, 25L, null);

            MessageInfo info = jsm.getMessage(R2, 1);
            assertStreamSource(info, S99, 26);

            sc = StreamConfiguration.builder()
                    .name(source(3))
                    .storageType(StorageType.Memory)
                    .sources(Source.builder().sourceName(S99).startSeq(11).filterSubject(S4).build())
                    .build();
            jsm.addStream(sc);
            assertSource(jsm, source(3), 20L, null);

            info = jsm.getMessage(source(3), 1);
            assertStreamSource(info, S99, 11);
        });
    }

    @Test
    public void testSourceAndTransformsRoundTrips() throws Exception {
        jsServer.run(si -> si.isNewerVersionThan("2.9.99"), nc -> {
            StreamConfiguration scMirror = StreamConfigurationTests.getStreamConfigurationFromJson("StreamConfigurationMirrorSubjectTransform.json");
            StreamConfiguration scSource = StreamConfigurationTests.getStreamConfigurationFromJson("StreamConfigurationSourcedSubjectTransform.json");

            JetStreamManagement jsm = nc.jetStreamManagement();
            StreamInfo si = jsm.addStream(scMirror);
            Mirror m = scMirror.getMirror();
            MirrorInfo mi = si.getMirrorInfo();
            assertEquals(m.getName(), mi.getName());
            assertEquals(m.getSubjectTransforms(), mi.getSubjectTransforms());
            assertTrue(scMirror.getSources().isEmpty());
            assertNull(si.getSourceInfos());
            jsm.deleteStream(scMirror.getName());

            si = jsm.addStream(scSource);
            assertNull(scSource.getMirror());
            assertNull(si.getMirrorInfo());

            Source source = scSource.getSources().get(0);
            SourceInfo info = si.getSourceInfos().get(0);
            assertEquals(1, info.getSubjectTransforms().size());

            assertEquals(source.getName(), info.getName());
            assertEquals(1, source.getSubjectTransforms().size());

            SubjectTransform st = source.getSubjectTransforms().get(0);
            SubjectTransform infoSt = info.getSubjectTransforms().get(0);
            assertEquals(st.getSource(), infoSt.getSource());
            assertEquals(st.getDestination(), infoSt.getDestination());

            source = scSource.getSources().get(1);
            info = si.getSourceInfos().get(1);
            assertEquals(source.getName(), info.getName());
            assertEquals(1, info.getSubjectTransforms().size());
            st = source.getSubjectTransforms().get(0);
            infoSt = info.getSubjectTransforms().get(0);
            assertEquals(st.getSource(), infoSt.getSource());
            assertEquals(st.getDestination(), infoSt.getDestination());

            jsm.deleteStream(scSource.getName());
        });
    }
}
