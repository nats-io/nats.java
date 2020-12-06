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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class JetstreamAPIResponseTests {

    @Test void testErrorResponses() {
        String msg = "Test generated error.";
        String json = "{\"type\" : \"thetype\",\"code\" : 1234, \"description\" : \"" + msg + "\"}";
        JetstreamAPIResponse resp = new JetstreamAPIResponse(json.getBytes());
        assertTrue(JetstreamAPIResponse.isError(json));
        assertTrue(resp.hasError());
        assertEquals(1234, resp.getCode());
        assertEquals("thetype", resp.getType());
        assertEquals(msg, resp.getDescription());
        assertEquals(json, resp.getResponse());
    }

    @Test
    public void testSuccessResponse() {
        String json = "{\"whatever\":\"value\"}";

        JetstreamAPIResponse resp = new JetstreamAPIResponse(json.getBytes());
        assertFalse(JetstreamAPIResponse.isError(json));
        assertEquals(-1, resp.getCode());
        assertEquals(null, resp.getDescription());
        assertFalse(resp.hasError());
        assertEquals(null, resp.getError());
        assertEquals(json, resp.getResponse());
    }
}