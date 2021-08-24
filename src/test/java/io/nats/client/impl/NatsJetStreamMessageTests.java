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
        Message jsMsg = getTestJsMessage();
        assertTrue(jsMsg.isJetStream());

        // two calls to msg.metaData are for coverage to test lazy initializer
        assertNotNull(jsMsg.metaData()); // this call takes a different path
        assertNotNull(jsMsg.metaData()); // this call shows that the lazy will work

        assertThrows(IllegalArgumentException.class, () -> jsMsg.ackSync(null));

        // cannot ackSync with no or negative duration
        assertThrows(IllegalArgumentException.class, () -> jsMsg.ackSync(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> jsMsg.ackSync(Duration.ofSeconds(-1)));

        assertThrows(IllegalStateException.class, () -> jsMsg.ackSync(Duration.ofSeconds(1)));
    }

    @Test
    public void testInvalid() {
        Message natsMsg = getTestNatsMessage();
        assertFalse(natsMsg.isJetStream());
        assertThrows(IllegalStateException.class, natsMsg::ack);
        assertThrows(IllegalStateException.class, natsMsg::nak);
        assertThrows(IllegalStateException.class, () -> natsMsg.ackSync(Duration.ofSeconds(42)));
        assertThrows(IllegalStateException.class, natsMsg::inProgress);
        assertThrows(IllegalStateException.class, natsMsg::term);
    }
}