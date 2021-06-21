// Copyright 2021 The NATS Authors
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

package io.nats.client.channels;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;

import org.junit.jupiter.api.Test;

import io.nats.client.Options;

public class NatsChannelTests {
    @Test
    public void testReference() throws IOException {
        NatsChannelReference ref = new NatsChannelReference(new BaseNatsChannel());
        assertTrue(ref.isOpen());
        ref.close();
        assertFalse(ref.isOpen());
        ref.set(new BaseNatsChannel());
        assertTrue(ref.isOpen());
        assertEquals(-1L, ref.write(new ByteBuffer[]{ByteBuffer.allocate(10)}));
    }

    @Test
    public void testSocketNatsChannel() throws Exception {
        NatsChannelFactory.Chain factory = SocketNatsChannel.factory();
        assertNull(factory.createSSLContext(new URI("tls://example.com")));
        assertThrows(
            ConnectTimeoutException.class,
            () -> factory.connect(new URI("nats://10.255.255.1"), Duration.ofMillis(10)));
    }
}