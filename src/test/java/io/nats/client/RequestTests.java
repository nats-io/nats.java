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

import io.nats.client.support.NatsException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class RequestTests {

    @Test
    public void testRequestNoResponder() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
            nc.request("isanybody.outhere", "hello world".getBytes()).get();
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getCause() instanceof NatsException);
            NatsException natsException = (NatsException)(e.getCause());
            assertEquals(503, natsException.getCode());
        }
    }
}