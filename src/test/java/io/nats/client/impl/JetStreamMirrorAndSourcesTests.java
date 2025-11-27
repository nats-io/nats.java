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
                    .mirror(mirror)
                    .build();
            ctx.addStream(sc);

            // Check the state
            assertMirror(ctx.jsm, S2, S1, 50L, 101L);

            jsPublish(ctx.js, U3, 100);

            // third mirror checks start seq
            sc = ctx.scBuilder()
                    .name(S3)
                    .mirror(Mirror.builder().sourceName(S1).startSeq(150).build())
                    .build();
            ctx.addStream(sc);

            // Check the state
            assertMirror(ctx.jsm, S3, S1, 101L, 150L);

            // third mirror checks start seq
            ZonedDateTime zdt = DateTimeUtils.fromNow(Duration.ofHours(-2));
            sc = ctx.scBuilder()
                    .name(S4)
                    .mirror(Mirror.builder().sourceName(S1).startTime(zdt).build())
                    .build();
            ctx.addStream(sc);

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
            StreamInfo si = ctx.addStream(sc);
            sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(S1, sc.getName());

            Mirror mirror = Mirror.builder().sourceName(S1).build();

            // Now create our mirror stream.
            sc = ctx.scBuilder()
                    .name(M1)
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
            assertThrows(JetStreamApiException.class, () -> ctx.addStream(scEx));
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

        runInSharedCustom((nc, ctx) -> {
            // Create streams
            StreamInfo si = ctx.addStream(ctx.scBuilder().name(N1).build());
            StreamConfiguration sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(N1, sc.getName());

            si = ctx.addStream(ctx.scBuilder().name(N2).build());
            sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(N2, sc.getName());

            si = ctx.addStream(ctx.scBuilder().name(N3).build());
            sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(N3, sc.getName());

            // Populate each one.
            jsPublish(ctx.js, N1, 10);
            jsPublish(ctx.js, N2, 15);
            jsPublish(ctx.js, N3, 25);

            sc = ctx.scBuilder()
                .name(R1)
                .sources(Source.builder().sourceName(N1).build(),
                    Source.builder().sourceName(N2).build(),
                    Source.builder().sourceName(N3).build())
                    .build();
            ctx.addStream(sc);

            assertSource(ctx.jsm, R1, 50L, null);

            sc = ctx.scBuilder()
                .name(R1)
                .sources(Source.builder().sourceName(N1).build(),
                    Source.builder().sourceName(N2).build(),
                    Source.builder().sourceName(N4).build())
                .build();

            ctx.jsm.updateStream(sc);

            sc = ctx.scBuilder(N4, U1)
                .name(N5)
                .build();
            ctx.addStream(sc);

            jsPublish(ctx.js, N4, 20);
            jsPublish(ctx.js, U1, 20);
            jsPublish(ctx.js, N4, 10);

            sc = ctx.scBuilder()
                .name(R2)
                .sources(Source.builder().sourceName(N5).startSeq(26).build())
                .build();
            ctx.addStream(sc);
            assertSource(ctx.jsm, R2, 25L, null);

            MessageInfo info = ctx.jsm.getMessage(R2, 1);
            assertStreamSource(info, N5, 26);

            sc = ctx.scBuilder()
                .name(N6)
                .sources(Source.builder().sourceName(N5).startSeq(11).filterSubject(N4).build())
                .build();
            ctx.addStream(sc);
            assertSource(ctx.jsm, N6, 20L, null);

            info = ctx.jsm.getMessage(N6, 1);
            assertStreamSource(info, N5, 11);
        });
    }

    @Test
    @Disabled("This used to work.")
    public void testSourceAndTransformsRoundTrips() throws Exception {
        runInOwnJsServer(VersionUtils::atLeast2_10, (nc, jsm, js) -> {
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
