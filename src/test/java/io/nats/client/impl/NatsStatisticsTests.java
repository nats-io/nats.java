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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

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
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

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
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                
                Dispatcher d = nc.createDispatcher((msg) -> {
                    nc.publish(msg.getReplyTo(), new byte[16]);
                });
                d.subscribe("subject");

                Future<Message> incoming = nc.request("subject", new byte[8]);
                Message msg = incoming.get(500, TimeUnit.MILLISECONDS);

                assertNotNull(msg);
                assertEquals("outstanding", 0, stats.getOutstandingRequests());
                assertTrue("bytes in", stats.getInBytes() > 100);
                assertTrue("bytes out", stats.getInBytes() > 100);
                assertEquals("messages in", 2, stats.getInMsgs()); // reply & request
                assertEquals("messages out", 6, stats.getOutMsgs()); // ping, sub, pub, msg, pub, msg
                assertEquals("oks", 5, stats.getOKs()); //sub, pub, msg, pub, msg
            } finally {
                nc.close();
            }
        }
    }
}