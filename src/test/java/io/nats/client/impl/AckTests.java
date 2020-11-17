// Copyright 2015-2018 The NATS Authors
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;

import org.junit.jupiter.api.Test;

public class AckTests {
    @Test
    public void testValidAck() {
        String json = "+OK {\"stream\":\"test\",\"seq\":42 }";

        try {
            Ack ack = new Ack(json.getBytes());
            assertEquals("test", ack.getStream());
            assertEquals(42, ack.getSeqno());
        }
        catch (Exception e) {
            fail("Unexpected Exception: " + e.getMessage());
        }
    }

    @Test
    public void testThrowsOnGarbage() {
        assertThrows(IOException.class, () -> {
            String json = "foo";
            new Ack(json.getBytes());
            assertFalse(true);
        });
    }
      
    @Test
    public void testThrowsOnERR() {
        String msg = "Test generated error.";
        try {
            String json = "-ERR " + msg;
            new Ack(json.getBytes());
            assertFalse(true);
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains(msg));
        }
    }

    @Test
    public void testInvalidResponse() {
        String json = "+OK {" +
                        "\"missing_stream\":\"test\"" + "," +
                        "\"missing_seq\":\"0\"" +
                       "}";   
        try {
            Ack ack = new Ack(json.getBytes());
            assertEquals(null, ack.getStream());
            assertEquals(-1, ack.getSeqno());
        }
        catch (Exception e) {
            fail("Unexpected Exception: " + e.getMessage());
        }
    }
}