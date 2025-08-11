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
import java.util.concurrent.atomic.AtomicInteger;

import static io.nats.client.utils.TestBase.*;
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
            StatisticsCollector stats = ((NatsConnection) nc).getStatisticsCollector();

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

    @Test
    public void testReadWriteAdvancedStatsEnabled() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server(ts.getURI()).verbose().turnOnAdvancedStats().build();
            Connection nc = Nats.connect(options);
            StatisticsCollector stats = ((NatsConnection) nc).getStatisticsCollector();

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

                // The read/write advanced stats are only exposed via toString, so assert on that
                String stringStats = stats.toString();
                assertTrue(stringStats.contains("Socket Reads"), "readStats count");
                assertTrue(stringStats.contains("Average Bytes Per Read"), "readStats average bytes");
                assertTrue(stringStats.contains("Min Bytes Per Read"), "readStats min bytes");
                assertTrue(stringStats.contains("Max Bytes Per Read"), "readStats max bytes");

                assertTrue(stringStats.contains("Socket Writes"), "writeStats count");
                assertTrue(stringStats.contains("Average Bytes Per Write"), "writeStats average bytes");
                assertTrue(stringStats.contains("Min Bytes Per Write"), "writeStats min bytes");
                assertTrue(stringStats.contains("Max Bytes Per Write"), "writeStats max bytes");
            } finally {
                nc.close();
            }
        }
    }

    @Test
    public void testReadWriteAdvancedStatsDisabled() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server(ts.getURI()).verbose().build();
            Connection nc = Nats.connect(options);
            StatisticsCollector stats = ((NatsConnection) nc).getStatisticsCollector();

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

                incoming = nc.request(m);
                incoming.get(500, TimeUnit.MILLISECONDS);

                assertNotNull(msg);

                incoming = nc.request(m);
                incoming.get(500, TimeUnit.MILLISECONDS);

                assertNotNull(msg);

                incoming = nc.request(m);
                incoming.get(500, TimeUnit.MILLISECONDS);

                assertNotNull(msg);

                // The read/write advanced stats are only exposed via toString, so assert on that
                String stringStats = stats.toString();
                assertFalse(stringStats.contains("Socket Reads"), "readStats count");
                assertFalse(stringStats.contains("Average Bytes Per Read"), "readStats average bytes");
                assertFalse(stringStats.contains("Min Bytes Per Read"), "readStats min bytes");
                assertFalse(stringStats.contains("Max Bytes Per Read"), "readStats max bytes");

                assertFalse(stringStats.contains("Socket Writes"), "writeStats count");
                assertFalse(stringStats.contains("Average Bytes Per Write"), "writeStats average bytes");
                assertFalse(stringStats.contains("Min Bytes Per Write"), "writeStats min bytes");
                assertFalse(stringStats.contains("Max Bytes Per Write"), "writeStats max bytes");
            } finally {
                nc.close();
            }
        }
    }

    @Test
    public void testOrphanDuplicateRepliesAdvancedStatsEnabled() throws Exception {
        Options.Builder builder = new Options.Builder().turnOnAdvancedStats();

        runInServer(builder, nc -> {
            AtomicInteger requests = new AtomicInteger();
            MessageHandler handler = (msg) -> {
                requests.incrementAndGet();
                nc.publish(msg.getReplyTo(), null);
            };
            Dispatcher d1 = nc.createDispatcher(handler);
            Dispatcher d2 = nc.createDispatcher(handler);
            Dispatcher d3 = nc.createDispatcher(handler);
            Dispatcher d4 = nc.createDispatcher(msg -> {
                sleep(5000);
                handler.onMessage(msg);
            });
            d1.subscribe(SUBJECT);
            d2.subscribe(SUBJECT);
            d3.subscribe(SUBJECT);
            d4.subscribe(SUBJECT);

            Message reply = nc.request(SUBJECT, null, Duration.ofSeconds(2));
            assertNotNull(reply);
            sleep(2000);
            assertEquals(3, requests.get());
            NatsStatistics stats = (NatsStatistics) nc.getStatistics();
            assertEquals(1, stats.getRepliesReceived());
            assertEquals(2, stats.getDuplicateRepliesReceived());
            assertEquals(0, stats.getOrphanRepliesReceived());

            sleep(3100);
            assertEquals(4, requests.get());
            stats = (NatsStatistics) nc.getStatistics();
            assertEquals(1, stats.getRepliesReceived());
            assertEquals(2, stats.getDuplicateRepliesReceived());
            assertEquals(1, stats.getOrphanRepliesReceived());

            String stringStats = stats.toString();
            assertTrue(stringStats.contains("Duplicate Replies Received"), "duplicate replies");
            assertTrue(stringStats.contains("Orphan Replies Received"), "orphan replies");
        });
    }

    @Test
    public void testOrphanDuplicateRepliesAdvancedStatsDisabled() throws Exception {
        Options.Builder builder = new Options.Builder();

        runInServer(builder, nc -> {
            AtomicInteger requests = new AtomicInteger();
            MessageHandler handler = (msg) -> {
                requests.incrementAndGet();
                nc.publish(msg.getReplyTo(), null);
            };
            Dispatcher d1 = nc.createDispatcher(handler);
            Dispatcher d2 = nc.createDispatcher(handler);
            Dispatcher d3 = nc.createDispatcher(handler);
            Dispatcher d4 = nc.createDispatcher(msg -> {
                sleep(5000);
                handler.onMessage(msg);
            });
            d1.subscribe(SUBJECT);
            d2.subscribe(SUBJECT);
            d3.subscribe(SUBJECT);
            d4.subscribe(SUBJECT);

            Message reply = nc.request(SUBJECT, null, Duration.ofSeconds(2));
            assertNotNull(reply);
            sleep(2000);
            assertEquals(3, requests.get());
            NatsStatistics stats = (NatsStatistics) nc.getStatistics();
            assertEquals(1, stats.getRepliesReceived());
            assertEquals(0, stats.getDuplicateRepliesReceived());
            assertEquals(0, stats.getOrphanRepliesReceived());

            sleep(3100);
            assertEquals(4, requests.get());
            stats = (NatsStatistics) nc.getStatistics();
            assertEquals(1, stats.getRepliesReceived());
            assertEquals(0, stats.getDuplicateRepliesReceived());
            assertEquals(0, stats.getOrphanRepliesReceived());

            String stringStats = stats.toString();
            assertFalse(stringStats.contains("Duplicate Replies Received"), "duplicate replies");
            assertFalse(stringStats.contains("Orphan Replies Received"), "orphan replies");
        });
    }
}
