// Copyright 2015-2018 The NATS Authors
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

import io.nats.client.Connection;
import io.nats.client.NatsTestServer;
import org.junit.jupiter.api.Test;

import static io.nats.client.utils.TestBase.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AuthAndConnectTests {
    @Test
    public void testIsAuthError() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Connection nc = standardConnection(ts.getURI());
            NatsConnection nats = (NatsConnection)nc;

            assertTrue(nats.isAuthenticationError("user authentication expired"));
            assertTrue(nats.isAuthenticationError("authorization violation"));
            assertTrue(nats.isAuthenticationError("Authorization Violation"));
            assertFalse(nats.isAuthenticationError("test"));
            assertFalse(nats.isAuthenticationError(""));
            assertFalse(nats.isAuthenticationError(null));

            standardCloseConnection(nc);
        }
    }

    @Test()
    public void testConnectWhenClosed() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            NatsConnection nc = (NatsConnection)standardConnection(ts.getURI());
            standardCloseConnection(nc);
            nc.connect(false); // should do nothing
            assertClosed(nc);
            nc.reconnect(); // should do nothing
            assertClosed(nc);
        }
    }
}