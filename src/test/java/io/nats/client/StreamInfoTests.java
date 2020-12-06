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

package io.nats.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import io.nats.client.StreamInfo.StreamState;

public class StreamInfoTests {

    @Test
    public void testJSONParsing() {
        String json = new StringBuilder().append("{ ")
            .append("\"type\": \"io.nats.jetstream.api.v1.stream_create_response\",")
            .append("\"config\": {")
            .append("  \"name\": \"sname\",")
            .append("  \"subjects\": [")
            .append("    \"foo\"")
            .append("  ],")
            .append("  \"retention\": \"limits\",")
            .append("  \"max_consumers\": -1,")
            .append("  \"max_msgs\": 9999,")
            .append("  \"max_bytes\": 10240,")
            .append("  \"discard\": \"new\",")
            .append("  \"max_age\": 7200000000000,")
            .append("  \"max_msg_size\": 1024,")
            .append("  \"storage\": \"memory\",")
            .append("  \"num_replicas\": 1,")
            .append("  \"duplicate_window\": 120000000000")
            .append("},")
            .append("\"created\": \"2020-11-29T23:34:45.051312Z\",")
            .append("\"state\": {")
            .append("  \"messages\": 11,")
            .append("  \"bytes\": 1234,")
            .append("  \"first_seq\": 11,")
            .append("  \"first_ts\": \"2020-11-29T23:44:45.051312Z\",")
            .append("  \"last_seq\": 22,")
            .append("  \"last_ts\":  \"2020-11-29T23:54:45.051312Z\",")
            .append("  \"consumer_count\": 42")
            .append("}")
            .append("}").toString();
          
        // check the info.
        StreamInfo info = new StreamInfo(json);
        assertEquals(34, info.getCreateTime().getMinute());


        // Config tests are elsewhere, so just spot check.
        StreamConfiguration config = info.getConfiguration();
        assertNotNull(config);
        assertEquals("sname", config.getName());
        assertEquals(Duration.ofHours(2), config.getMaxAge());

        // check state
        StreamState state = info.getStreamState();
        assertNotNull(state);
        assertEquals(1234, state.getByteCount());
        assertEquals(42, state.getConsumerCount());
        assertEquals(11, state.getFirstSequence());        
        assertEquals(44, state.getFirstTime().getMinute());
        assertEquals(22, state.getLastSequence());
        assertEquals(54, state.getLastTime().getMinute());
        assertEquals(11, state.getMsgCount());
    }

}