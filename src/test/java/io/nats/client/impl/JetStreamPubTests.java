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

import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.PublishAck;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class JetStreamPubTests extends JetStreamTestBase {

    @Test
    public void testPub() throws Exception {
        runInJsServer(nc -> {
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            Message msg = NatsMessage.builder()
                    .subject(SUBJECT)
                    .data(DATA.getBytes(StandardCharsets.US_ASCII))
                    .build();

            PublishAck pa = js.publish(msg);
        });
    }

    @Test
    public void testPublishAckJson() throws IOException {
        String json = "{\"stream\":\"sname\", \"seq\":1, \"duplicate\":false}";
        PublishAck pa = new NatsPublishAck(json.getBytes(StandardCharsets.US_ASCII));
        assertEquals("sname", pa.getStream());
        assertEquals(1, pa.getSeqno());
        assertFalse(pa.isDuplicate());
        assertNotNull(pa.toString());
    }
}
