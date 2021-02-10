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

import io.nats.client.Message;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

public class NatsJetStreamMessageTests extends JetStreamTestBase {

    @Test
    public void testMiscCoverage() {
        Message msg = getJsMessage(JS_REPLY_TO);
        assertTrue(msg.isJetStream());

        // two calls to msg.metaData are for coverage to test lazy initializer
        assertNotNull(msg.metaData()); // this call takes a different path
        assertNotNull(msg.metaData()); // this call shows that the lazy will work

        assertThrows(IllegalArgumentException.class, () -> msg.ackSync(null));

        // cannot ackSync with no or negative duration
        assertThrows(IllegalArgumentException.class, () -> msg.ackSync(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> msg.ackSync(Duration.ofSeconds(-1)));

        assertThrows(IllegalStateException.class, () -> msg.ackSync(Duration.ofSeconds(1)));
    }

    @Test
    public void testInvalid() {
        Message m = new NatsMessage.IncomingMessageFactory("sid", "subj", "replyTo", 0, false).getMessage();
        assertFalse(m.isJetStream());
        assertThrows(IllegalStateException.class, m::ack);
        assertThrows(IllegalStateException.class, m::nak);
        assertThrows(IllegalStateException.class, () -> m.ackSync(Duration.ofSeconds(42)));
        assertThrows(IllegalStateException.class, m::inProgress);
        assertThrows(IllegalStateException.class, m::term);
    }
}