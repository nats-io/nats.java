// Copyright 2025 The NATS Authors
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

import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.nats.client.support.JsonUtils;
import io.nats.client.support.Status;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static io.nats.client.support.DateTimeUtils.DEFAULT_TIME;
import static io.nats.client.support.NatsJetStreamConstants.*;
import static io.nats.client.support.Status.EOB;
import static org.junit.jupiter.api.Assertions.*;

public class MessageInfoTest {

    @Test
    public void testMessageInfoComprehensive() {
        // Test 1: Constructor with Status
        Status testStatus = new Status(404, "Not Found");
        String streamName = "test-stream";
        MessageInfo statusMessageInfo = new MessageInfo(testStatus, streamName);

        assertTrue(statusMessageInfo.isStatus());
        assertFalse(statusMessageInfo.isMessage());
        assertFalse(statusMessageInfo.isEobStatus());
        assertTrue(statusMessageInfo.isErrorStatus());
        assertEquals(testStatus, statusMessageInfo.getStatus());
        assertEquals(streamName, statusMessageInfo.getStream());
        assertNull(statusMessageInfo.getSubject());
        assertEquals(-1, statusMessageInfo.getSeq());
        assertNull(statusMessageInfo.getData());
        assertNull(statusMessageInfo.getTime());
        assertNull(statusMessageInfo.getHeaders());
        assertEquals(-1, statusMessageInfo.getLastSeq());
        assertEquals(-1, statusMessageInfo.getNumPending());

        // Test 2: Constructor with EOB Status
        MessageInfo eobMessageInfo = new MessageInfo(EOB, streamName);
        assertTrue(eobMessageInfo.isEobStatus());
        assertFalse(eobMessageInfo.isErrorStatus());

        // Test 3: Constructor with direct parsing (Message with headers)
        Headers headers = new Headers();
        headers.put(NATS_SUBJECT, "test.subject");
        headers.put(NATS_SEQUENCE, "12345");
        headers.put(NATS_TIMESTAMP, "2023-01-01T12:00:00Z");
        headers.put(NATS_STREAM, "direct-stream");
        headers.put(NATS_LAST_SEQUENCE, "67890");
        headers.put(NATS_NUM_PENDING, "6");
        headers.put("custom-header", "custom-value");

        byte[] testData = "test message data".getBytes();
        Message directMessage = new NatsMessage("test.subject", null, headers, testData);

        MessageInfo testInfo = new MessageInfo(directMessage, "override-stream", true);

        assertEquals("test.subject", testInfo.getSubject());
        assertEquals(12345L, testInfo.getSeq());
        assertArrayEquals(testData, testInfo.getData());
        assertNotNull(testInfo.getTime());
        assertEquals("direct-stream", testInfo.getStream());
        assertEquals(67890L, testInfo.getLastSeq());
        assertEquals(5L, testInfo.getNumPending()); // NUM_PENDING - 1
        assertNotNull(testInfo.getHeaders());
        assertEquals("custom-value", testInfo.getHeaders().getFirst("custom-header"));
        assertNull(testInfo.getHeaders().getFirst(NATS_SUBJECT)); // Control headers removed
        assertTrue(testInfo.isMessage());
        assertFalse(testInfo.isStatus());
        assertNull(testInfo.getStatus());

        // Test 4: Constructor with direct parsing (Message without headers)
        // This pathway us a no-header direct
        Message noHeaderMessage = new NatsMessage("test.subject", null, null, testData);
        MessageInfo noHeaderMessageInfo = new MessageInfo(noHeaderMessage, streamName, true);

        assertNull(noHeaderMessageInfo.getSubject());
        assertEquals(-1L, noHeaderMessageInfo.getSeq());
        assertArrayEquals(testData, noHeaderMessageInfo.getData());
        assertNull(noHeaderMessageInfo.getTime());
        assertEquals(streamName, noHeaderMessageInfo.getStream());
        assertEquals(-1L, noHeaderMessageInfo.getLastSeq());
        assertEquals(-1L, noHeaderMessageInfo.getNumPending());
        assertNotNull(noHeaderMessageInfo.getHeaders());
        assertTrue(noHeaderMessageInfo.isMessage());

        // Test 5: Constructor with JSON parsing (simulated with valid JSON message)
        String jsonPayload = JsonUtils.beginJson()
            .append("\"message\":{")
            .append("\"subject\":\"json.subject\",")
            .append("\"seq\":54321,")
            .append("\"data\":\"").append(Base64.getEncoder().encodeToString("json data".getBytes())).append("\",")
            .append("\"time\":\"2023-01-01T15:30:00Z\"")
            .append("}")
            .append("}").toString();

        Message jsonMessage = new NatsMessage("response.subject", null, null, jsonPayload.getBytes());
        MessageInfo jsonMessageInfo = new MessageInfo(jsonMessage, "json-stream", false);

        assertEquals("json.subject", jsonMessageInfo.getSubject());
        assertEquals(54321L, jsonMessageInfo.getSeq());
        assertArrayEquals("json data".getBytes(), jsonMessageInfo.getData());
        assertNotNull(jsonMessageInfo.getTime());
        assertEquals("json-stream", jsonMessageInfo.getStream());
        assertTrue(jsonMessageInfo.isMessage());

        // Test 6: Deprecated constructor for COVERAGE
        @SuppressWarnings("deprecation")
        MessageInfo deprecatedMessageInfo = new MessageInfo(directMessage);
        assertTrue(deprecatedMessageInfo.hasError());
        assertFalse(deprecatedMessageInfo.isMessage());
        assertNull(deprecatedMessageInfo.getStatus());

        // Test 7: Test toString() method for different types
        String statusString = statusMessageInfo.toString();
        assertTrue(statusString.contains("MessageInfo"));
        assertTrue(statusString.contains("status_code"));

        String messageString = testInfo.toString();
        assertTrue(messageString.contains("MessageInfo"));
        assertTrue(messageString.contains("seq"));
        assertTrue(messageString.contains("subject"));

        // Test 8: Edge cases with invalid sequence numbers
        Headers invalidHeaders = new Headers();
        invalidHeaders.put(NATS_SEQUENCE, "invalid");
        invalidHeaders.put(NATS_LAST_SEQUENCE, "invalid");
        invalidHeaders.put(NATS_NUM_PENDING, "invalid");
        invalidHeaders.put(NATS_TIMESTAMP, "invalid");
        Message invalidMessage = new NatsMessage("test.subject", null, invalidHeaders, testData);
        MessageInfo mi = new MessageInfo(invalidMessage, streamName, true);
        assertEquals(-1, mi.getSeq());
        assertEquals(-1, mi.getLastSeq());
        assertEquals(-1, mi.getNumPending());
        assertEquals(DEFAULT_TIME, mi.getTime());

        // Test 9: Test with error message
        String errorJson = "{\"error\":{\"code\":400,\"description\":\"Bad Request\"}}";
        Message errorMessage = new NatsMessage("error.subject", null, null, errorJson.getBytes());
        MessageInfo errorMessageInfo = new MessageInfo(errorMessage, streamName, false);

        assertFalse(errorMessageInfo.isMessage());
        assertTrue(errorMessageInfo.hasError());

        // Test 10: Verify all method combinations work correctly
        assertNotNull(testInfo.getSubject());
        assertTrue(testInfo.getSeq() > 0);
        assertNotNull(testInfo.getData());
        assertNotNull(testInfo.getTime());
        assertNotNull(testInfo.getHeaders());
        assertNotNull(testInfo.getStream());
        assertTrue(testInfo.getLastSeq() > 0);
        assertTrue(testInfo.getNumPending() >= 0);
        assertNull(testInfo.getStatus());
        assertTrue(testInfo.isMessage());
        assertFalse(testInfo.isStatus());
        assertFalse(testInfo.isEobStatus());
        assertFalse(testInfo.isErrorStatus());
        assertFalse(testInfo.hasError());
    }
}