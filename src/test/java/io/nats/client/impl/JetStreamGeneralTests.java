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

import java.time.Duration;

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
            createTestStream(nc);
            JetStream js = nc.jetStream();
            publish(js);

            // default ephemeral subscription.
            Subscription s = js.subscribe(SUBJECT);
            Message m = s.nextMessage(Duration.ofSeconds(1));
            assertNotNull(m);
            assertEquals(DATA, new String(m.getData()));

            // default subscribe options // ephemeral subscription.
            s = js.subscribe(SUBJECT, PushSubscribeOptions.defaultInstance());
            m = s.nextMessage(Duration.ofSeconds(1));
            assertNotNull(m);
            assertEquals(DATA, new String(m.getData()));

            // set the stream
            PushSubscribeOptions so = PushSubscribeOptions.builder().stream(STREAM).build();
            s = js.subscribe(SUBJECT, so);
            m = s.nextMessage(Duration.ofSeconds(1));
            assertNotNull(m);
            assertEquals(DATA, new String(m.getData()));

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
    public void testConsumerInPullModeRequiresExplicitAckPolicy() throws Exception {
        runInJsServer(nc -> {
            createTestStream(nc);
            JetStream js = nc.jetStream();

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .ackPolicy(ConsumerConfiguration.AckPolicy.All).build();
            PullSubscribeOptions pullOptsAll = PullSubscribeOptions.builder()
                    .defaultBatchSize(1)
                    .durable(DURABLE)
                    .configuration(cc).build();
            assertThrows(JetStreamApiException.class, () -> js.subscribe(SUBJECT, pullOptsAll));

            cc = ConsumerConfiguration.builder()
                    .ackPolicy(ConsumerConfiguration.AckPolicy.None).build();
            PullSubscribeOptions pullOptsNone = PullSubscribeOptions.builder()
                    .defaultBatchSize(1)
                    .durable(DURABLE)
                    .configuration(cc).build();
            assertThrows(JetStreamApiException.class, () -> js.subscribe(SUBJECT, pullOptsNone));
        });
    }

}
