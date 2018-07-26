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

package io.nats.client;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Duration;

import org.junit.Test;

import io.nats.client.NatsServerProtocolMock.ExitAt;

public class EchoTests {
    @Test(expected=IOException.class)
    public void testFailWithBadServerProtocol() throws Exception {
        Connection nc = null;
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.NO_EXIT)) {
            Options opt = new Options.Builder().server(ts.getURI()).noEcho().noReconnect().build();
            try {
                nc = Nats.connect(opt); // Should fail
            } finally {
                if (nc != null) {
                    nc.close();
                    assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
                }
            }
        }
    }

    @Test
    public void testConnectToOldServerWithEcho() throws Exception {
        Connection nc = null;
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.NO_EXIT)) {
            Options opt = new Options.Builder().server(ts.getURI()).noReconnect().build();
            try {
                nc = Nats.connect(opt);
            } finally {
                if (nc != null) {
                    nc.close();
                    assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
                }
            }
        }
    }
    
    @Test
    public void testWithEcho() throws Exception {
        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = new Options.Builder().server(ts.getURI()).noReconnect().build();
            try (Connection nc1 = Nats.connect(options);
                    Connection nc2 = Nats.connect(options);) {

                // Echo is on so both sub should get messages from both pub
                Subscription sub1 = nc1.subscribe("test");
                nc1.flush(Duration.ofSeconds(1));
                Subscription sub2 = nc2.subscribe("test");
                nc2.flush(Duration.ofSeconds(1));

                // Pub from connect 1
                nc1.publish("test", null);
                nc1.flush(Duration.ofSeconds(1));
                Message msg = sub1.nextMessage(Duration.ofSeconds(1));
                assertNotNull(msg);
                msg = sub2.nextMessage(Duration.ofSeconds(1));
                assertNotNull(msg);

                // Pub from connect 2
                nc2.publish("test", null);
                nc2.flush(Duration.ofSeconds(1));
                msg = sub1.nextMessage(Duration.ofSeconds(1));
                assertNotNull(msg);
                msg = sub2.nextMessage(Duration.ofSeconds(1));
                assertNotNull(msg);
            }
        }
    }
    
    @Test
    public void testWithNoEcho() throws Exception {
        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = new Options.Builder().server(ts.getURI()).noEcho().noReconnect().build();
            try (Connection nc1 = Nats.connect(options);
                    Connection nc2 = Nats.connect(options);) {

                // Echo is on so both sub should get messages from both pub
                Subscription sub1 = nc1.subscribe("test");
                nc1.flush(Duration.ofSeconds(1));
                Subscription sub2 = nc2.subscribe("test");
                nc2.flush(Duration.ofSeconds(1));

                // Pub from connect 1
                nc1.publish("test", null);
                nc1.flush(Duration.ofSeconds(1));
                Message msg = sub1.nextMessage(Duration.ofSeconds(1));
                assertNull(msg); // no message for sub1 from pub 1
                msg = sub2.nextMessage(Duration.ofSeconds(1));
                assertNotNull(msg);

                // Pub from connect 2
                nc2.publish("test", null);
                nc2.flush(Duration.ofSeconds(1));
                msg = sub1.nextMessage(Duration.ofSeconds(1));
                assertNotNull(msg);
                msg = sub2.nextMessage(Duration.ofSeconds(1));
                assertNull(msg); // no message for sub2 from pub 2
            }
        }
    }
}