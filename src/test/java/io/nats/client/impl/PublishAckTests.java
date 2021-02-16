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

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class PublishAckTests {
    @Test
    public void testValidAck() {
        String json = "{\"stream\":\"test\",\"seq\":42, \"duplicate\" : true }";

        try {
            NatsPublishAck ack = new NatsPublishAck(json.getBytes());
            assertEquals("test", ack.getStream());
            assertEquals(42, ack.getSeqno());
            assertTrue(ack.isDuplicate());
        }
        catch (Exception e) {
            fail("Unexpected Exception: " + e.getMessage());
        }
    }

    @Test
    public void testThrowsOnGarbage() {
        assertThrows(IOException.class, () -> {
            new NatsPublishAck("notjson".getBytes());
       });
    }
      
    @Test
    public void testThrowsOnERR() {
        String json ="{" +
                "  \"type\": \"io.nats.jetstream.api.v1.pub_ack_response\"," +
                "  \"error\": {" +
                "    \"code\": 500," +
                "    \"description\": \"the description\"" +
                "  }" +
                "}";

        JetStreamApiException jsapi = assertThrows(JetStreamApiException.class, () -> new NatsPublishAck(json.getBytes()));
        assertEquals(500, jsapi.getErrorCode());
    }

    @Test
    public void testInvalidResponse() {
        String json = "+OK {" +
                        "\"missing_stream\":\"test\"" + "," +
                        "\"missing_seq\":\"0\"" +
                       "}";

        IOException ioe = assertThrows(IOException.class, () -> new NatsPublishAck(json.getBytes()));
        assertEquals("Invalid ack from a JetStream publish", ioe.getMessage());
    }
}