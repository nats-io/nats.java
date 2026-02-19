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

package io.nats.client.api;

import io.nats.client.JetStreamApiException;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.nats.client.utils.TestBase.getDataMessage;
import static org.junit.jupiter.api.Assertions.*;

public class PublishAckTests {
    @Test
    public void testAllFieldsSet() {
        String json = "{" +
            "\"stream\":\"test-stream\"," +
            "\"seq\":42," +
            "\"domain\":\"test-domain\"," +
            "\"duplicate\":true," +
            "\"val\":\"-73\"," +
            "\"batch\":\"batch-id\"," +
            "\"count\":66" +
            "}";

        try {
            PublishAck ack = new PublishAck(getDataMessage(json));
            assertEquals("test-stream", ack.getStream());
            assertEquals("test-domain", ack.getDomain());
            assertEquals(42, ack.getSeqno());
            assertTrue(ack.isDuplicate());
            assertEquals("-73", ack.getVal());
            assertEquals("batch-id", ack.getBatchId());
            assertEquals(66, ack.getBatchSize());
        }
        catch (Exception e) {
            fail("Unexpected Exception: " + e.getMessage());
        }
    }

    @Test
    public void testRequiredFieldsSet() {
        String json = "{\"stream\":\"test-stream\",\"seq\":0}";
        try {
            PublishAck ack = new PublishAck(getDataMessage(json));
            assertEquals("test-stream", ack.getStream());
            assertEquals(0, ack.getSeqno());
            assertNull(ack.getDomain());
            assertFalse(ack.isDuplicate());
            assertNull(ack.getVal());
            assertNull(ack.getBatchId());
            assertEquals(-1, ack.getBatchSize());
        }
        catch (Exception e) {
            fail("Unexpected Exception: " + e.getMessage());
        }
    }

    @Test
    public void testThrowsOnGarbage() {
        assertThrows(JetStreamApiException.class, () -> new PublishAck(getDataMessage("notjson")));
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

        JetStreamApiException jsapi = assertThrows(JetStreamApiException.class,
                () -> new PublishAck(getDataMessage(json)).throwOnHasError());
        assertEquals(500, jsapi.getErrorCode());
    }

    @Test
    public void testInvalidResponse() {
        IOException ioe = assertThrows(IOException.class,
            () -> new PublishAck(getDataMessage("{\"stream\":\"no sequence\"}")));
        assertEquals("Invalid JetStream ack.", ioe.getMessage());

        ioe = assertThrows(IOException.class,
            () -> new PublishAck(getDataMessage("{\"seq\":1}")));
        assertEquals("Invalid JetStream ack.", ioe.getMessage());
    }
}