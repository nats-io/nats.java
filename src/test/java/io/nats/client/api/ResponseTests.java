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

import io.nats.client.impl.JetStreamTestBase;
import io.nats.client.support.DateTimeUtils;
import java.time.Duration;
import org.junit.jupiter.api.Test;

import static io.nats.client.utils.ResourceUtils.dataAsString;
import static org.junit.jupiter.api.Assertions.*;

public class ResponseTests extends JetStreamTestBase {

    @Test
    public void testPurgeResponse() {
        String json = dataAsString("PurgeResponse.json");
        PurgeResponse pr = new PurgeResponse(getDataMessage(json));
        assertTrue(pr.isSuccess());
        assertEquals(5, pr.getPurged());
        //noinspection deprecation
        assertEquals(5, pr.getPurgedCount()); // coverage for deprecated
        assertNotNull(pr.toString()); // COVERAGE
    }

    @Test
    public void testPauseResponse() {
        String json = dataAsString("ConsumerPauseResponse.json");
        ConsumerPauseResponse pr = new ConsumerPauseResponse(getDataMessage(json));
        assertTrue(pr.isPaused());
        assertEquals(DateTimeUtils.parseDateTime("2024-03-02T13:21:45.198423724Z"), pr.getPauseUntil());
        assertEquals(Duration.ofSeconds(30), pr.getPauseRemaining());
        assertNotNull(pr.toString()); // COVERAGE
    }

    @Test
    public void testPauseResumeResponse() {
        String json = dataAsString("ConsumerResumeResponse.json");
        ConsumerPauseResponse pr = new ConsumerPauseResponse(getDataMessage(json));
        assertFalse(pr.isPaused());
        assertEquals(DateTimeUtils.parseDateTime("0001-01-01T00:00:00Z"), pr.getPauseUntil());
        assertNull(pr.getPauseRemaining());
        assertNotNull(pr.toString()); // COVERAGE
    }
}
