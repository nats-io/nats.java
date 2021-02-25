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
            s = js.subscribe(SUBJECT, PushSubscribeOptions.defaultInstance());
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

            // another subscription with valid filter subject
            sub = js.subscribe(SUBJECT, pso1);
            assertSubscription(sub, STREAM, DURABLE, null, false);

            // filtering on a subject that the durable does not already filter
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(subject(9), pso1));

            // filter subject ignored when consumer already exists
            ConsumerConfiguration cc2 = ConsumerConfiguration.builder().filterSubject("ignored-bc-not-creating").durable(DURABLE).build();
            PushSubscribeOptions pso2 = PushSubscribeOptions.builder().configuration(cc2).build();
            sub = js.subscribe(SUBJECT, pso2);
            assertSubscription(sub, STREAM, DURABLE, null, false);
        });
    }
}
