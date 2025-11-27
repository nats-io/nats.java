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
import io.nats.client.utils.SharedServer;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;

import static io.nats.client.utils.ConnectionUtils.standardConnectionWait;
import static io.nats.client.utils.OptionsUtils.optionsBuilder;
import static org.junit.jupiter.api.Assertions.*;

public class EchoTests extends TestBase {
    @Test
    public void testFailWithBadServerProtocol() {
        assertThrows(IOException.class, () -> {
            Connection nc = null;
            try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(ExitAt.NO_EXIT)) {
                Options opt = optionsBuilder(mockTs.getMockUri()).noEcho().noReconnect().build();
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
        try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(ExitAt.NO_EXIT)) {
            Options opt = optionsBuilder(mockTs.getMockUri()).noReconnect().build();
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
        runInShared(nc1 -> {
            try (Connection nc2 = standardConnectionWait(optionsBuilder(nc1).build())) {
                // Echo is on so both sub should get messages from both pub
                String subject = random();
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
        });
    }

    @Test
    public void testWithNoEcho() throws Exception {
        runInSharedOwnNc(optionsBuilder().noEcho().noReconnect(), nc1 -> {
            Connection nc2 = SharedServer.sharedConnectionForSameServer(nc1);

            String subject = random();
            Subscription sub1 = nc1.subscribe(subject);
            nc1.flush(Duration.ofSeconds(1));
            Subscription sub2 = nc2.subscribe(subject);
            nc2.flush(Duration.ofSeconds(1));

            // Pub from connect 1
            nc1.publish(subject, null);
            nc1.flush(Duration.ofSeconds(1));
            Message msg = sub1.nextMessage(Duration.ofSeconds(1));
            assertNull(msg); // no message for sub1 from pub 1
            msg = sub2.nextMessage(Duration.ofSeconds(1));
            assertNotNull(msg);

            // Pub from connect 2
            nc2.publish(subject, null);
            nc2.flush(Duration.ofSeconds(1));
            msg = sub1.nextMessage(Duration.ofSeconds(1));
            assertNotNull(msg);
            msg = sub2.nextMessage(Duration.ofSeconds(1));
            assertNotNull(msg); // nc2 is not no echo
        });
    }
}