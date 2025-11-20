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

import io.nats.client.NatsServerProtocolMock.ExitAt;
import io.nats.client.utils.LongRunningServer;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;

import static io.nats.client.utils.TestOptions.NOOP_EL;
import static org.junit.jupiter.api.Assertions.*;

public class EchoTests {
    @Test
    public void testFailWithBadServerProtocol() {
        assertThrows(IOException.class, () -> {
            Connection nc = null;
            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.NO_EXIT)) {
                Options opt = new Options.Builder().server(ts.getURI()).noEcho().noReconnect().errorListener(NOOP_EL).build();
                try {
                    nc = Nats.connect(opt); // Should fail
                }
                finally {
                    if (nc != null) {
                        nc.close();
                        assertSame(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
                    }
                }
            }
        });
    }

    @Test
    public void testConnectToOldServerWithEcho() throws Exception {
        Connection nc = null;
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.NO_EXIT)) {
            Options opt = new Options.Builder().server(ts.getURI()).noReconnect().errorListener(NOOP_EL).build();
            try {
                nc = Nats.connect(opt);
            } finally {
                if (nc != null) {
                    nc.close();
                    assertSame(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
                }
            }
        }
    }
    
    @Test
    public void testWithEcho() throws Exception {
        // do not open LrConns in try-resources
        Connection nc1 = LongRunningServer.getLrConn();
        Connection nc2 = LongRunningServer.getLrConn2();
        // Echo is on so both sub should get messages from both pub
        String subject = TestBase.random();
        Subscription sub1 = nc1.subscribe(subject);
        nc1.flush(Duration.ofSeconds(1));
        Subscription sub2 = nc2.subscribe(subject);
        nc2.flush(Duration.ofSeconds(1));

        // Pub from connect 1
        nc1.publish(subject, null);
        nc1.flush(Duration.ofSeconds(1));
        Message msg = sub1.nextMessage(Duration.ofSeconds(1));
        assertNotNull(msg);
        msg = sub2.nextMessage(Duration.ofSeconds(1));
        assertNotNull(msg);

        // Pub from connect 2
        nc2.publish(subject, null);
        nc2.flush(Duration.ofSeconds(1));
        msg = sub1.nextMessage(Duration.ofSeconds(1));
        assertNotNull(msg);
        msg = sub2.nextMessage(Duration.ofSeconds(1));
        assertNotNull(msg);
    }

    @Test
    public void testWithNoEcho() throws Exception {
        Options options = LongRunningServer.optionsBuilder().noEcho().noReconnect().build();
        try (Connection nc1 = Nats.connect(options);) {
            String subject = TestBase.random();
            // Echo is off so sub should get messages from pub from other connections
            Subscription sub1 = nc1.subscribe(subject);
            nc1.flush(Duration.ofSeconds(1));

            // Pub from connect 1
            nc1.publish(subject, null);
            nc1.flush(Duration.ofSeconds(1));
            Message msg = sub1.nextMessage(Duration.ofSeconds(1));
            assertNull(msg); // no message for sub1 from pub 1
        }
    }
}