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

import static io.nats.client.utils.ResourceUtils.dataAsString;
import static org.junit.jupiter.api.Assertions.*;

public class JetStreamApiResponseTests {

    @Test
    public void testErrorResponse() {
        String text = dataAsString("ErrorResponses.json.txt");
        String[] jsons = text.split("~");

        JetStreamApiResponse jsApiResp = new JetStreamApiResponse(jsons[0].getBytes());
        assertTrue(jsApiResp.hasError());
        assertEquals("code_and_desc_response", jsApiResp.getType());
        assertEquals(500, jsApiResp.getErrorCode());
        assertEquals("the description", jsApiResp.getDescription());
        assertEquals("the description (500)", jsApiResp.getError());

        jsApiResp = new JetStreamApiResponse(jsons[1].getBytes());
        assertTrue(jsApiResp.hasError());
        assertEquals("zero_and_desc_response", jsApiResp.getType());
        assertEquals(0, jsApiResp.getErrorCode());
        assertEquals("the description", jsApiResp.getDescription());
        assertEquals("the description (0)", jsApiResp.getError());

        jsApiResp = new JetStreamApiResponse(jsons[2].getBytes());
        assertTrue(jsApiResp.hasError());
        assertEquals("non_zero_code_only_response", jsApiResp.getType());
        assertEquals(500, jsApiResp.getErrorCode());
        assertNull(jsApiResp.getDescription());
        assertEquals("Unknown Jetstream Error (500)", jsApiResp.getError());

        jsApiResp = new JetStreamApiResponse(jsons[3].getBytes());
        assertTrue(jsApiResp.hasError());
        assertEquals("no_code_response", jsApiResp.getType());
        assertEquals(JetStreamApiResponse.NOT_SET, jsApiResp.getErrorCode());
        assertEquals("no code", jsApiResp.getDescription());
        assertEquals("no code (-1)", jsApiResp.getError());

        jsApiResp = new JetStreamApiResponse(jsons[4].getBytes());
        assertTrue(jsApiResp.hasError());
        assertEquals("empty_response", jsApiResp.getType());
        assertEquals(JetStreamApiResponse.NOT_SET, jsApiResp.getErrorCode());
        assertNull(jsApiResp.getDescription());
        assertTrue(jsApiResp.getError().startsWith("Unknown Jetstream Error:"));
        assertTrue(jsApiResp.getError().contains(jsApiResp.getResponse()));

        jsApiResp = new JetStreamApiResponse(jsons[5].getBytes());
        assertFalse(jsApiResp.hasError());
        assertEquals("not_error_response", jsApiResp.getType());
    }
}