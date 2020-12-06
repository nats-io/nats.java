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

import java.time.Duration;

import org.junit.jupiter.api.Test;

import io.nats.client.ConsumerConfiguration.AckPolicy;
import io.nats.client.ConsumerConfiguration.DeliverPolicy;
import io.nats.client.ConsumerConfiguration.ReplayPolicy;

public class ConsumerInfoTests {

    @Test
    public void testJSONParsing() {
        String json = "{\n  \"type\": \"io.nats.jetstream.api.v1.consumer_info_response\",\n  \"stream_name\": \"foo-stream\",\n  \"name\": \"foo-consumer\",\n  \"created\": \"2020-11-05T19:33:21.163377Z\",\n  \"config\": {\n    \"durable_name\": \"foo-consumer\",\n    \"deliver_subject\": \"bar\",\n    \"deliver_policy\": \"all\",\n    \"ack_policy\": \"all\",\n    \"ack_wait\": 30000000000,\n    \"max_deliver\": 10,\n    \"replay_policy\": \"original\"\n  },\n  \"delivered\": {\n    \"consumer_seq\": 1,\n    \"stream_seq\": 2\n  },\n  \"ack_floor\": {\n    \"consumer_seq\": 3,\n    \"stream_seq\": 4\n  },\n  \"num_pending\": 24,,\n  \"num_ack_pending\": 42,\n  \"num_redelivered\": 42\n}";

        ConsumerInfo info = new ConsumerInfo(json);

        assertEquals("foo-stream", info.getStreamName());
        assertEquals("foo-consumer", info.getName());

        assertEquals(1, info.getDelivered().getConsumerSequence());
        assertEquals(2, info.getDelivered().getStreamSequence());
        assertEquals(3, info.getAckFloor().getConsumerSequence());
        assertEquals(4, info.getAckFloor().getStreamSequence());

        assertEquals(24, info.getNumPending());
        assertEquals(42, info.getNumAckPending());
        assertEquals(42, info.getRedelivered());

        ConsumerConfiguration c = info.getConsumerConfiguration();
        assertEquals("foo-consumer", c.getDurable());
        assertEquals("bar", c.getDeliverSubject());
        assertEquals(DeliverPolicy.All, c.getDeliverPolicy());
        assertEquals(AckPolicy.All, c.getAckPolicy());
        assertEquals(Duration.ofSeconds(30), c.getAckWait());
        assertEquals(10, c.getMaxDeliver());
        assertEquals(ReplayPolicy.Original, c.getReplayPolicy());
    }

}