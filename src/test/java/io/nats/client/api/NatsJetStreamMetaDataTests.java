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

import io.nats.client.Message;
import io.nats.client.impl.JetStreamTestBase;
import io.nats.client.impl.NatsJetStreamMetaData;
import io.nats.client.impl.NatsMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class NatsJetStreamMetaDataTests extends JetStreamTestBase {

    @Test
    public void testMetaData() {
        Message msg = getTestMessage(TestMetaV0);
        NatsJetStreamMetaData meta = msg.metaData(); // first time, coverage lazy check is null
        assertNotNull(msg.metaData()); // 2nd time, coverage lazy check is not null
        assertNotNull(meta.toString()); // COVERAGE toString

        validateMeta(false, false, getTestMessage(TestMetaV0).metaData());
        validateMeta(true, false, getTestMessage(TestMetaV1).metaData());
        validateMeta(true, true, getTestMessage(TestMetaV2).metaData());
    }

    private void validateMeta(boolean hasPending, boolean hasDomainHashToken, NatsJetStreamMetaData meta) {
        assertEquals("test-stream", meta.getStream());
        assertEquals("test-consumer", meta.getConsumer());
        assertEquals(1, meta.deliveredCount());
        assertEquals(2, meta.streamSequence());
        assertEquals(3, meta.consumerSequence());

        assertEquals(2020, meta.timestamp().getYear());
        assertEquals(6, meta.timestamp().getMinute());
        assertEquals(113260000, meta.timestamp().getNano());

        assertEquals(hasPending ? 4L : -1L, meta.pendingCount());

        if (hasDomainHashToken) {
            assertEquals("v2Domain", meta.getDomain());
            assertEquals("v2Hash", meta.getAccountHash());
            assertEquals("v2Token", meta.getToken());

        }
        else {
            assertNull(meta.getDomain());
            assertNull(meta.getAccountHash());
            assertNull(meta.getToken());
        }
    }

    @Test
    public void testInvalidMetaDataConstruction() {
        assertThrows(IllegalArgumentException.class,
                () -> new NatsJetStreamMetaData(NatsMessage.builder().subject("test").build()));

        assertThrows(IllegalArgumentException.class,
                () -> new NatsJetStreamMetaData(getTestMessage("$JS.ACK.not.enough.parts")));

        assertThrows(IllegalArgumentException.class,
                () -> new NatsJetStreamMetaData(getTestMessage(TestMetaV0 + ".too.many.parts")));

        assertThrows(IllegalArgumentException.class,
                () -> new NatsJetStreamMetaData(getTestMessage("$JS.ZZZ.enough.parts.though.need.three.more")));

        assertThrows(IllegalArgumentException.class,
                () -> new NatsJetStreamMetaData(new NatsMessage("sub", null, new byte[0])));
    }
}
