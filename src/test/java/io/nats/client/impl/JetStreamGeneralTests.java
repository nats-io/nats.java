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

import static io.nats.examples.NatsJsUtils.printStreamInfo;
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

    public static final String ABC1 = "A.B.C.1";
    public static final String ABC2 = "A.B.C.2";
    public static final String ABC3 = "A.B.C.3";
    public static final String ABX1 = "A.B.X.1";
    public static final String ABX2 = "A.B.X.2";
    public static final String ABX3 = "A.B.X.3";

    // @Test
    public void filters() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            StreamInfo si = createMemoryStream(nc, STREAM, ABC1, ABC2, ABC3, ABX1, ABX2, ABX3);
            printStreamInfo(si);

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .durable(DURABLE)
                    .filterSubject("A.B.C.>").build();

            JetStreamManagement jsm = nc.jetStreamManagement();
            jsm.addConsumer(STREAM, cc);

            PushSubscribeOptions pso = PushSubscribeOptions.builder().durable(DURABLE).build();
            JetStreamSubscription sub = js.subscribe(ABC1, pso);
            assertSubscription(sub, STREAM, null, null, false);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

        });
    }
}
