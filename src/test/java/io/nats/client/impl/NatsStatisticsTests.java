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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.NatsTestServer;
import io.nats.client.Options;
import io.nats.client.Nats;

public class NatsStatisticsTests {
    @Test
    public void testHumanReadableString() throws Exception {
        // This test is purely for coverage, any test without a human is likely pedantic
        try (NatsTestServer ts = new NatsTestServer();
                Connection nc = Nats.connect(new Options.Builder()
                                                .server(ts.getURI())
                                                .turnOnAdvancedStats()
                                                .build())) {
            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

            Dispatcher d = nc.createDispatcher((msg) -> {
                nc.publish(msg.getReplyTo(), new byte[16]);
            });
            d.subscribe("subject");

            nc.flush(Duration.ofMillis(500));
            Future<Message> incoming = nc.request("subject", new byte[8]);
            nc.flush(Duration.ofMillis(500));
            Message msg = incoming.get(500, TimeUnit.MILLISECONDS);

            String str = nc.getStatistics().toString();
            assertNotNull(msg);
            assertNotNull(str);
            assertTrue(str.length() > 0);
            assertTrue(str.contains("### Connection ###"));
            assertTrue(str.contains("Socket Writes"));
        }
    }

    @Test
    public void testInOutOKRequestStats() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server(ts.getURI()).verbose().build();
            Connection nc = Nats.connect(options);
            NatsStatistics stats = ((NatsConnection) nc).getNatsStatistics();

            try {
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
                
                Dispatcher d = nc.createDispatcher((msg) -> {
                    nc.publish(msg.getReplyTo(), new byte[16]);
                });
                d.subscribe("subject");

                Future<Message> incoming = nc.request("subject", new byte[8]);
                Message msg = incoming.get(500, TimeUnit.MILLISECONDS);

                assertNotNull(msg);
                assertEquals(0, stats.getOutstandingRequests(), "outstanding");
                assertTrue(stats.getInBytes() > 100, "bytes in");
                assertTrue(stats.getInBytes() > 100, "bytes out");
                assertEquals(2, stats.getInMsgs(), "messages in"); // reply & request
                assertEquals(6, stats.getOutMsgs(), "messages out"); // ping, sub, pub, msg, pub, msg
                assertEquals(5, stats.getOKs(), "oks"); //sub, pub, msg, pub, msg
            } finally {
                nc.close();
            }
        }
    }
}