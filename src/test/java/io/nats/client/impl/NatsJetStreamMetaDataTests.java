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
import io.nats.client.MessageMetaData;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class NatsJetStreamMetaDataTests extends JetStreamTestBase {

    @Test
    public void testMetaData() {
        Message msg = getJsMessage(JS_REPLY_TO);

        // two calls to msg.metaData are for coverage to test lazy initializer
        MessageMetaData jsmd = msg.metaData(); // this call takes a different path
        assertNotNull(msg.metaData()); // this call shows that the lazy will work

        assertEquals("test-stream", jsmd.getStream());
        assertEquals("test-consumer", jsmd.getConsumer());
        assertEquals(1, jsmd.deliveredCount());
        assertEquals(2, jsmd.streamSequence());
        assertEquals(3, jsmd.consumerSequence());
        assertEquals(2020, jsmd.timestamp().getYear());
        assertEquals(6, jsmd.timestamp().getMinute());
        assertEquals(113260000, jsmd.timestamp().getNano());
        assertEquals(-1, jsmd.pendingCount());

        jsmd = getJsMessage(JS_REPLY_TO + ".555").metaData();
        assertEquals(555, jsmd.pendingCount());

        assertNotNull(jsmd.toString()); // COVERAGE
    }

    @Test
    public void testInvalidMetaDataConstruction() {
        assertThrows(IllegalArgumentException.class,
                () -> new NatsJetStreamMetaData(NatsMessage.builder().subject("test").build()));

        assertThrows(IllegalArgumentException.class,
                () -> new NatsJetStreamMetaData(getJsMessage("$JS.ACK.not.enough.parts")));

        assertThrows(IllegalArgumentException.class,
                () -> new NatsJetStreamMetaData(getJsMessage(JS_REPLY_TO + ".too.many.parts")));

        assertThrows(IllegalArgumentException.class,
                () -> new NatsJetStreamMetaData(getJsMessage("$JS.ZZZ.enough.parts.though.need.three.more")));

        assertThrows(IllegalArgumentException.class,
                () -> new NatsJetStreamMetaData(new NatsMessage("sub", null, new byte[0], false)));
    }
}
