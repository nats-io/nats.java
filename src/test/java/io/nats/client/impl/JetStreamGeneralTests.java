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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

public class JetStreamGeneralTests extends JetStreamTestBase {

    @Test
    public void testJetEnabled() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false, false); Connection nc = Nats.connect(ts.getURI())) {
            IllegalStateException ise = assertThrows(IllegalStateException.class, nc::jetStream);
            assertEquals("JetStream is not enabled.", ise.getMessage());
        }
    }

    @Test
    public void testJetStreamPublishDefaultOptions() throws Exception {
        runInJsServer(nc -> {
            createTestStream(nc);
            JetStream js = nc.jetStream();
            PublishAck ack = publish(js);
            assertEquals(1, ack.getSeqno());
        });
    }

    @Test
    public void testConnectionClosing() throws Exception {
        runInJsServer(nc -> {
            nc.close();
            assertThrows(IOException.class, nc::jetStream);
        });
    }

    @Test
    public void testCreateWithOptionsForCoverage() throws Exception {
        runInJsServer(nc -> {
            JetStreamOptions jso = JetStreamOptions.builder().build();
            nc.jetStream(jso);
        });
    }

    @Test
    public void notJetStream() {
        NatsMessage m = NatsMessage.builder().subject("test").build();
        assertThrows(IllegalStateException.class, m::ack);
        assertThrows(IllegalStateException.class, m::nak);
        assertThrows(IllegalStateException.class, () -> m.ackSync(Duration.ZERO));
        assertThrows(IllegalStateException.class, m::inProgress);
        assertThrows(IllegalStateException.class, m::term);
        assertThrows(IllegalStateException.class, m::metaData);
    }

    @Test
    public void testJetStreamSubscribe() throws Exception {
        runInJsServer(nc -> {
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            createTestStream(jsm);
            publish(js);

            // default ephemeral subscription.
            Subscription s = js.subscribe(SUBJECT);
            Message m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(DATA, new String(m.getData()));
            List<String> names = jsm.getConsumerNames(STREAM);
            assertEquals(1, names.size());

            // default subscribe options // ephemeral subscription.
            s = js.subscribe(SUBJECT, PushSubscribeOptions.builder().build());
            m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(DATA, new String(m.getData()));
            names = jsm.getConsumerNames(STREAM);
            assertEquals(2, names.size());

            // set the stream
            PushSubscribeOptions pso = PushSubscribeOptions.builder().stream(STREAM).build();
            s = js.subscribe(SUBJECT, pso);
            m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(DATA, new String(m.getData()));
            names = jsm.getConsumerNames(STREAM);
            assertEquals(3, names.size());

            System.out.println(names);
        });
    }

    @Test
    public void testNoMatchingStreams() throws Exception {
        runInJsServer(nc -> {
            JetStream js = nc.jetStream();
            assertThrows(IllegalStateException.class, () -> js.subscribe(SUBJECT));
        });
    }

    @Test
    public void testFiltersSubscribing() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT, subject(9));

            // first pass creates the consumer
            ConsumerConfiguration cc1 = ConsumerConfiguration.builder().filterSubject(SUBJECT).durable(DURABLE).build();
            PushSubscribeOptions pso1 = PushSubscribeOptions.builder().configuration(cc1).build();
            JetStreamSubscription sub = js.subscribe(SUBJECT, pso1);
            assertSubscription(sub, STREAM, DURABLE, null, false);

            JetStreamManagement jsm = nc.jetStreamManagement();
            debug(jsm, 1);

            // another subscription with valid filter subject
            sub = js.subscribe(SUBJECT, pso1);
            assertSubscription(sub, STREAM, DURABLE, null, false);
            debug(jsm, 2);

            // filtering on a subject that the durable does not already filter
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(subject(9), pso1));

            // filter subject ignored when consumer already exists
            ConsumerConfiguration cc2 = ConsumerConfiguration.builder().filterSubject("ignored-bc-not-creating").durable(DURABLE).build();
            PushSubscribeOptions pso2 = PushSubscribeOptions.builder().configuration(cc2).build();
            sub = js.subscribe(SUBJECT, pso2);
            assertSubscription(sub, STREAM, DURABLE, null, false);
            debug(jsm, 3);
        });
    }

    @Test
    public void testPrefix() throws Exception {
        String prefix = "tar.api";
        String streamMadeBySrc = "stream-made-by-src";
        String streamMadeByTar = "stream-made-by-tar";
        String subjectMadeBySrc = "sub-made-by.src";
        String subjectMadeByTar = "sub-made-by.tar";

        try (NatsTestServer ts = new NatsTestServer("src/test/resources/js_prefix.conf", false)) {
            Options optionsSrc = new Options.Builder().server(ts.getURI())
                    .userInfo("src".toCharArray(), "spass".toCharArray()).build();

            Options optionsTar = new Options.Builder().server(ts.getURI())
                    .userInfo("tar".toCharArray(), "tpass".toCharArray()).build();

            try (Connection ncSrc = Nats.connect(optionsSrc);
                 Connection ncTar = Nats.connect(optionsTar)
            ) {
                // Setup JetStreamOptions. SOURCE does not need prefix
                JetStreamOptions jsoSrc = JetStreamOptions.builder().build();
                JetStreamOptions jsoTar = JetStreamOptions.builder().prefix(prefix).build();

                // Management api allows us to create streams
                JetStreamManagement jsmSrc = ncSrc.jetStreamManagement(jsoSrc);
                JetStreamManagement jsmTar = ncTar.jetStreamManagement(jsoTar);

                // add streams with both account
                StreamConfiguration scSrc = StreamConfiguration.builder()
                        .name(streamMadeBySrc)
                        .storageType(StreamConfiguration.StorageType.Memory)
                        .subjects(subjectMadeBySrc)
                        .build();
                jsmSrc.addStream(scSrc);

                StreamConfiguration scTar = StreamConfiguration.builder()
                        .name(streamMadeByTar)
                        .storageType(StreamConfiguration.StorageType.Memory)
                        .subjects(subjectMadeByTar)
                        .build();
                jsmTar.addStream(scTar);

                JetStream jsSrc = ncSrc.jetStream(jsoSrc);
                JetStream jsTar = ncTar.jetStream(jsoTar);

                jsSrc.publish(subjectMadeBySrc, "src-src".getBytes());
                jsSrc.publish(subjectMadeByTar, "src-tar".getBytes());
                jsTar.publish(subjectMadeBySrc, "tar-src".getBytes());
                jsTar.publish(subjectMadeByTar, "tar-tar".getBytes());

                // subscribe and read messages
                readPrefixMessages(ncSrc, jsSrc, subjectMadeBySrc, "src");
                readPrefixMessages(ncSrc, jsSrc, subjectMadeByTar, "tar");
                readPrefixMessages(ncTar, jsTar, subjectMadeBySrc, "src");
                readPrefixMessages(ncTar, jsTar, subjectMadeByTar, "tar");
            }
        }
    }

    private void readPrefixMessages(Connection nc, JetStream js, String subject, String dest) throws InterruptedException, IOException, JetStreamApiException, TimeoutException {
        JetStreamSubscription sub = js.subscribe(subject);
        nc.flush(Duration.ofSeconds(1));
        List<Message> msgs = readMessagesAck(sub);
        assertEquals(2, msgs.size());
        assertEquals(subject, msgs.get(0).getSubject());
        assertEquals(subject, msgs.get(1).getSubject());

        assertEquals("src-" + dest, new String(msgs.get(0).getData()));
        assertEquals("tar-" + dest, new String(msgs.get(1).getData()));
    }


    @Test
    public void testPrefixManager() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> JsPrefixManager.addPrefix(HAS_DOLLAR));
        assertThrows(IllegalArgumentException.class, () -> JsPrefixManager.addPrefix(HAS_GT));
        assertThrows(IllegalArgumentException.class, () -> JsPrefixManager.addPrefix(HAS_STAR));

        JsPrefixManager.addPrefix("foo");
        JsPrefixManager.addPrefix("bar.");

        assertTrue(JsPrefixManager.hasPrefix(NatsJetStreamConstants.JS_PREFIX));
        assertTrue(JsPrefixManager.hasPrefix(NatsJetStreamConstants.JSAPI_PREFIX));
        assertTrue(JsPrefixManager.hasPrefix("foo.blah"));
        assertTrue(JsPrefixManager.hasPrefix("bar.blah"));
        assertFalse(JsPrefixManager.hasPrefix("not"));
    }
}