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

        runInSharedCustom((nc, ctx) -> {
            Mirror mirror = Mirror.builder().sourceName(S1).build();

            // Create source stream
            StreamConfiguration sc = ctx.scBuilder(U1, U2, U3).name(S1).build();
            StreamInfo si = ctx.addStream(sc);
            sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(S1, sc.getName());

            // Now create our mirror stream.
            sc = ctx.scBuilder()
                .name(M1)
                .subjects() // scBuilder added subjects
                .mirror(mirror)
                .build();
            ctx.addStream(sc);
            assertMirror(ctx.jsm, M1, S1, null, null);

            // Send 100 messages.
            jsPublish(ctx.js, U2, 100);

            // Check the state
            assertMirror(ctx.jsm, M1, S1, 100L, null);

            // Purge the source stream.
            ctx.jsm.purgeStream(S1);

            jsPublish(ctx.js, U2, 50);

            // Create second mirror
            sc = ctx.scBuilder()
                .name(S2)
                .subjects() // scBuilder added subjects
                .mirror(mirror)
                .build();
            ctx.createOrReplaceStream(sc);

            // Check the state
            assertMirror(ctx.jsm, S2, S1, 50L, 101L);

            jsPublish(ctx.js, U3, 100);

            // third mirror checks start seq
            sc = ctx.scBuilder()
                .name(S3)
                .subjects() // scBuilder added subjects
                .mirror(Mirror.builder().sourceName(S1).startSeq(150).build())
                .build();
            ctx.createOrReplaceStream(sc);

            // Check the state
            assertMirror(ctx.jsm, S3, S1, 101L, 150L);

            // third mirror checks start seq
            ZonedDateTime zdt = DateTimeUtils.fromNow(Duration.ofHours(-2));
            sc = ctx.scBuilder()
                .name(S4)
                .subjects() // scBuilder added subjects
                .mirror(Mirror.builder().sourceName(S1).startTime(zdt).build())
                .build();
            ctx.createOrReplaceStream(sc);

            // Check the state
            assertMirror(ctx.jsm, S4, S1, 150L, 101L);
        });
    }

    @Test
    public void testMirrorReading() throws Exception {
        String S1 = random();
        String U1 = random();
        String U2 = random();
        String M1 = random();

        runInSharedCustom((nc, ctx) -> {
            // Create source stream
            StreamConfiguration sc = ctx.scBuilder(U1, U2)
                    .name(S1)
                    .build();
            StreamInfo si = ctx.createOrReplaceStream(sc);
            sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(S1, sc.getName());

            Mirror mirror = Mirror.builder().sourceName(S1).build();

            // Now create our mirror stream.
            sc = ctx.scBuilder()
                .name(M1)
                .subjects() // scBuilder added subjects
                .mirror(mirror)
                .build();
            ctx.addStream(sc);
            assertMirror(ctx.jsm, M1, S1, null, null);

            // Send messages.
            jsPublish(ctx.js, U1, 10);
            jsPublish(ctx.js, U2, 20);

            assertMirror(ctx.jsm, M1, S1, 30L, null);

            JetStreamSubscription sub = ctx.js.subscribe(U1);
            List<Message> list = readMessagesAck(sub);
            assertEquals(10, list.size());
            for (Message m : list) {
                assertEquals(S1, m.metaData().getStream());
            }

            sub = ctx.js.subscribe(U2);
            list = readMessagesAck(sub);
            assertEquals(20, list.size());
            for (Message m : list) {
                assertEquals(S1, m.metaData().getStream());
            }

            //noinspection deprecation
            PushSubscribeOptions.bind(M1); // coverage for deprecated
            PushSubscribeOptions pso = PushSubscribeOptions.stream(M1);
            sub = ctx.js.subscribe(U1, pso);
            list = readMessagesAck(sub);
            assertEquals(10, list.size());
            for (Message m : list) {
                assertEquals(M1, m.metaData().getStream());
            }

            sub = ctx.js.subscribe(U2, pso);
            list = readMessagesAck(sub);
            assertEquals(20, list.size());
            for (Message m : list) {
                assertEquals(M1, m.metaData().getStream());
            }
        });
    }

    @Test
    public void testMirrorExceptions() throws Exception {
        runInSharedCustom((nc, ctx) -> {
            Mirror mirror = Mirror.builder().sourceName(random()).build();
            StreamConfiguration scEx = StreamConfiguration.builder()
                    .name(random())
                    .subjects(random())
                    .mirror(mirror)
                    .build();
            assertThrows(JetStreamApiException.class, () -> ctx.createOrReplaceStream(scEx));
        });
    }

    @Test
    public void testSourceBasics() throws Exception {
        String S1 = random();
        String S2 = random();
        String S3 = random();
        String S4 = random();
        String S5 = random();
        String S99 = random();
        String R1 = random();
        String R2 = random();

        runInSharedCustom((nc, ctx) -> {
            // Create streams
            StreamInfo si = ctx.addStream(StreamConfiguration.builder()
                    .name(S1).storageType(StorageType.Memory).build());
            StreamConfiguration sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(S1, sc.getName());

            si = ctx.addStream(StreamConfiguration.builder()
                    .name(S2).storageType(StorageType.Memory).build());
            sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(S2, sc.getName());

            si = ctx.addStream(StreamConfiguration.builder()
                    .name(S3).storageType(StorageType.Memory).build());
            sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(S3, sc.getName());

            // Populate each one.
            jsPublish(ctx.js, S1, 10);
            jsPublish(ctx.js, S2, 15);
            jsPublish(ctx.js, S3, 25);

            sc = StreamConfiguration.builder()
                    .name(R1)
                    .storageType(StorageType.Memory)
                    .sources(Source.builder().sourceName(S1).build(),
                            Source.builder().sourceName(S2).build(),
                            Source.builder().sourceName(S3).build())
                    .build();

            ctx.addStream(sc);

            assertSource(ctx.jsm, R1, 50L, null);

            sc = StreamConfiguration.builder()
                    .name(R1)
                    .storageType(StorageType.Memory)
                    .sources(Source.builder().sourceName(S1).build(),
                            Source.builder().sourceName(S2).build(),
                            Source.builder().sourceName(S4).build())
                    .build();

            ctx.jsm.updateStream(sc);

            sc = StreamConfiguration.builder()
                    .name(S99)
                    .storageType(StorageType.Memory)
                    .subjects(S4, S5)
                    .build();
            ctx.addStream(sc);

            jsPublish(ctx.js, S4, 20);
            jsPublish(ctx.js, S5, 20);
            jsPublish(ctx.js, S4, 10);

            sc = StreamConfiguration.builder()
                    .name(R2)
                    .storageType(StorageType.Memory)
                    .sources(Source.builder().sourceName(S99).startSeq(26).build())
                    .build();
            ctx.addStream(sc);
            assertSource(ctx.jsm, R2, 25L, null);

            MessageInfo info = ctx.jsm.getMessage(R2, 1);
            assertStreamSource(info, S99, 26);

            String source3 = random();
            sc = StreamConfiguration.builder()
                    .name(source3)
                    .storageType(StorageType.Memory)
                    .sources(Source.builder().sourceName(S99).startSeq(11).filterSubject(S4).build())
                    .build();
            ctx.addStream(sc);
            assertSource(ctx.jsm, source3, 20L, null);

            info = ctx.jsm.getMessage(source3, 1);
            assertStreamSource(info, S99, 11);
        });
    }

    @Test
    public void testSourceAndTransformsRoundTrips() throws Exception {
        runInOwnJsServer(VersionUtils::atLeast2_10, (nc, jsm, js) -> {
            StreamConfiguration sc = StreamConfigurationTests.getStreamConfigurationFromJson(
                "StreamConfigurationSourcedSubjectTransform.json");

            assertNotNull(sc.getSources());
            assertNull(sc.getMirror());

            Source sourceFoo = sc.getSources().get(0);
            Source sourceBar = sc.getSources().get(1);

            StreamInfo si = jsm.addStream(sc);
            assertNotNull(si.getSourceInfos());
            assertNull(si.getMirrorInfo());

            for (SourceInfo info : si.getSourceInfos()) {
                assertNotNull(info);
                assertNotNull(info.getSubjectTransforms());
                assertEquals(1, info.getSubjectTransforms().size());

                String which = info.getName().substring(7); // sourcedfoo --> foo sourcebar --> bar
                Source source;
                if (which.equals("foo")) {
                    source = sourceFoo;
                }
                else {
                    source = sourceBar;
                }
                assertEquals(source.getName(), info.getName());
                assertNotNull(source.getSubjectTransforms());
                assertEquals(1, source.getSubjectTransforms().size());

                SubjectTransform st = source.getSubjectTransforms().get(0);
                SubjectTransform infoSt = info.getSubjectTransforms().get(0);
                assertEquals(st.getSource(), infoSt.getSource());
                assertEquals(st.getDestination(), infoSt.getDestination());
            }
        });
    }

    @Test
    public void testMirror() throws Exception {
        runInOwnJsServer(VersionUtils::atLeast2_10, (nc, jsm, js) -> {
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
