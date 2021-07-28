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
import io.nats.client.support.Ulong;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.nats.client.utils.TestBase.getDataMessage;
import static org.junit.jupiter.api.Assertions.*;

public class PublishAckTests {
    @Test
    public void testValidAck() {
        String json = "{\"stream\":\"test\",\"seq\":42, \"duplicate\" : true }";

        try {
            PublishAck ack = new PublishAck(getDataMessage(json));
            assertEquals("test", ack.getStream());
            assertEquals(new Ulong(42), ack.getSequenceNum());
            assertTrue(ack.isDuplicate());
        }
        catch (Exception e) {
            fail("Unexpected Exception: " + e.getMessage());
        }
    }

    @Test
    public void testThrowsOnGarbage() {
        assertThrows(IOException.class, () -> {
            new PublishAck(getDataMessage("notjson"));
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

        JetStreamApiException jsapi = assertThrows(JetStreamApiException.class,
                () -> new PublishAck(getDataMessage(json)).throwOnHasError());
        assertEquals(500, jsapi.getErrorCode());
    }

    @Test
    public void testInvalidResponse() {
        String json1 = "+OK {" +
                        "\"missing_stream\":\"test\"" + "," +
                        "\"missing_seq\":\"0\"" +
                       "}";

        IOException ioe = assertThrows(IOException.class, () -> new PublishAck(getDataMessage(json1)));
        assertEquals("Invalid JetStream ack.", ioe.getMessage());

        String json2 = "{\"stream\":\"test\", \"duplicate\" : true }";
        ioe = assertThrows(IOException.class, () -> new PublishAck(getDataMessage(json2)));
    }
}