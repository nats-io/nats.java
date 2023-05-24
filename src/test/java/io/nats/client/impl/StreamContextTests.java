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

import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.StreamContext;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerInfo;
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
            assertNotNull(streamContext.getStreamInfo());
            assertNotNull(streamContext.getStreamInfo(StreamInfoOptions.builder().build()));

            assertThrows(JetStreamApiException.class, () -> streamContext.getConsumerContext(DURABLE));
            assertThrows(JetStreamApiException.class, () -> streamContext.deleteConsumer(DURABLE));

            ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(DURABLE).build();
            ConsumerInfo ci = streamContext.createConsumer(cc);
            assertEquals(STREAM, ci.getStreamName());
            assertEquals(DURABLE, ci.getName());

            assertNotNull(streamContext.getConsumerContext(DURABLE));
            streamContext.deleteConsumer(DURABLE);

            assertThrows(JetStreamApiException.class, () -> streamContext.getConsumerContext(DURABLE));
            assertThrows(JetStreamApiException.class, () -> streamContext.deleteConsumer(DURABLE));
        });
    }
}
