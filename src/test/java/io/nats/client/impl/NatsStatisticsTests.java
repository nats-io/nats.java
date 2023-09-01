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

import io.nats.client.*;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class NatsStatisticsTests {
    @Test
    public void testHumanReadableString() throws Exception {
        // This test is purely for coverage, any test without a human is likely pedantic
        try (NatsTestServer ts = new NatsTestServer();
                Connection nc = Nats.connect(new Options.Builder()
                                                .server(ts.getURI())
                                                .turnOnAdvancedStats()
                                                .build())) {
            assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");

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
            StatisticsCollector stats = ((NatsConnection) nc).getNatsStatistics();

            try {
                assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                
                Dispatcher d = nc.createDispatcher((msg) -> {
                    Message m = NatsMessage.builder()
                        .subject(msg.getReplyTo())
                        .data(new byte[16])
                        .headers(new Headers().put("header", "reply"))
                        .build();
                    nc.publish(m);
                });
                d.subscribe("subject");

                Message m = NatsMessage.builder()
                    .subject("subject")
                    .data(new byte[8])
                    .headers(new Headers().put("header", "request"))
                    .build();
                Future<Message> incoming = nc.request(m);
                Message msg = incoming.get(500, TimeUnit.MILLISECONDS);

                assertNotNull(msg);
                assertEquals(0, stats.getOutstandingRequests(), "outstanding");
                assertTrue(stats.getInBytes() > 200, "bytes in");
                assertTrue(stats.getOutBytes() > 400, "bytes out");
                assertEquals(2, stats.getInMsgs(), "messages in"); // reply & request
                assertEquals(6, stats.getOutMsgs(), "messages out"); // ping, sub, pub, msg, pub, msg
                assertEquals(5, stats.getOKs(), "oks"); //sub, pub, msg, pub, msg
            } finally {
                nc.close();
            }
        }
    }
}
