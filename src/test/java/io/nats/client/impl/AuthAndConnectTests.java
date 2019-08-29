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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.NatsTestServer;

public class AuthAndConnectTests {
    @Test
    public void testIsAuthError() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Connection nc = Nats.connect(ts.getURI());
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

                NatsConnection nats = (NatsConnection)nc;

                assertTrue(nats.isAuthenticationError("user authentication expired"));
                assertTrue(nats.isAuthenticationError("authorization violation"));
                assertTrue(nats.isAuthenticationError("Authorization Violation"));
                assertFalse(nats.isAuthenticationError("test"));
                assertFalse(nats.isAuthenticationError(""));
                assertFalse(nats.isAuthenticationError(null));
                
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test()
    public void testConnectWhenClosed() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Connection nc = Nats.connect(ts.getURI());
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

                NatsConnection nats = (NatsConnection)nc;

                nats.close();
                assertTrue("Connected Status", Connection.Status.CLOSED == nc.getStatus());
                nats.connect(false); // should do nothing
                assertTrue("Connected Status", Connection.Status.CLOSED == nc.getStatus());
                nats.reconnect(); // should do nothing
                assertTrue("Connected Status", Connection.Status.CLOSED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }
}