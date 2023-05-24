// Copyright 2023 The NATS Authors
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
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.MessageInfo;
import io.nats.client.api.StreamInfoOptions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class StreamContextTests extends JetStreamTestBase {

    @Test
    public void testStreamContext() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            assertThrows(JetStreamApiException.class, () -> js.getStreamContext(STREAM));

            createDefaultTestStream(jsm);

            StreamContext streamContext = js.getStreamContext(STREAM);
            assertEquals(STREAM, streamContext.getStreamName());

            assertThrows(JetStreamApiException.class, () -> streamContext.getConsumerContext(DURABLE));
            assertThrows(JetStreamApiException.class, () -> streamContext.deleteConsumer(DURABLE));

            ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(DURABLE).build();
            ConsumerInfo ci = streamContext.addConsumer(cc);
            assertEquals(STREAM, ci.getStreamName());
            assertEquals(DURABLE, ci.getName());

            ci = streamContext.getConsumerInfo(DURABLE);
            assertNotNull(ci);
            assertEquals(STREAM, ci.getStreamName());
            assertEquals(DURABLE, ci.getName());

            assertEquals(1, streamContext.getConsumerNames().size());

            assertEquals(1, streamContext.getConsumers().size());
            assertNotNull(streamContext.getConsumerContext(DURABLE));
            streamContext.deleteConsumer(DURABLE);

            assertThrows(JetStreamApiException.class, () -> streamContext.getConsumerContext(DURABLE));
            assertThrows(JetStreamApiException.class, () -> streamContext.deleteConsumer(DURABLE));

            // coverage
            js.publish(SUBJECT, "one".getBytes());
            js.publish(SUBJECT, "two".getBytes());
            js.publish(SUBJECT, "three".getBytes());
            js.publish(SUBJECT, "four".getBytes());
            js.publish(SUBJECT, "five".getBytes());
            js.publish(SUBJECT, "six".getBytes());

            assertTrue(streamContext.deleteMessage(3));
            assertTrue(streamContext.deleteMessage(4, true));

            MessageInfo mi = streamContext.getMessage(1);
            assertEquals(1, mi.getSeq());

            mi = streamContext.getFirstMessage(SUBJECT);
            assertEquals(1, mi.getSeq());

            mi = streamContext.getLastMessage(SUBJECT);
            assertEquals(6, mi.getSeq());

            mi = streamContext.getNextMessage(3, SUBJECT);
            assertEquals(5, mi.getSeq());

            assertNotNull(streamContext.getStreamInfo());
            assertNotNull(streamContext.getStreamInfo(StreamInfoOptions.builder().build()));

            streamContext.purge(PurgeOptions.builder().sequence(5).build());
            assertThrows(JetStreamApiException.class, () -> streamContext.getMessage(1));

            mi = streamContext.getFirstMessage(SUBJECT);
            assertEquals(5, mi.getSeq());

            streamContext.purge();
            assertThrows(JetStreamApiException.class, () -> streamContext.getFirstMessage(SUBJECT));
        });
    }
}
