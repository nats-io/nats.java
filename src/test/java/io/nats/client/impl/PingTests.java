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
import io.nats.client.ConnectionListener.Events;
import io.nats.client.NatsServerProtocolMock.ExitAt;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.*;

import static io.nats.client.utils.TestBase.runInJsServer;
import static org.junit.jupiter.api.Assertions.*;

public class PingTests {
    @Test
    public void testHandlingPing() throws Exception,ExecutionException {
        CompletableFuture<Boolean> gotPong = new CompletableFuture<>();

        NatsServerProtocolMock.Customizer pingPongCustomizer = (ts, r,w) -> {
            
            System.out.println("*** Mock Server @" + ts.getPort() + " sending PING ...");
            w.write("PING\r\n");
            w.flush();

            String pong = "";
            
            System.out.println("*** Mock Server @" + ts.getPort() + " waiting for PONG ...");
            try {
                pong = r.readLine();
            } catch(Exception e) {
                gotPong.cancel(true);
                return;
            }

            if (pong.startsWith("PONG")) {
                System.out.println("*** Mock Server @" + ts.getPort() + " got PONG ...");
                gotPong.complete(Boolean.TRUE);
            } else {
                System.out.println("*** Mock Server @" + ts.getPort() + " got something else... " + pong);
                gotPong.complete(Boolean.FALSE);
            }
        };

        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(pingPongCustomizer)) {
            Connection  nc = Nats.connect(ts.getURI());
            try {
                assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                assertTrue(gotPong.get(), "Got pong.");
            } finally {
                nc.close();
                assertSame(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testPingTimer() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server(ts.getURI())
                .pingInterval(Duration.ofMillis(5))
                .maxPingsOut(10000) // just don't want this to be what fails the test
                .build();
            NatsConnection nc = (NatsConnection) Nats.connect(options);
            StatisticsCollector stats = nc.getStatisticsCollector();

            try {
                assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                try { Thread.sleep(200); } catch (Exception ignore) {} // 1200 / 100 ... should get 10+ pings
                assertTrue(stats.getPings() > 10, "got pings");
            } finally {
                nc.close();
                assertSame(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testPingFailsWhenClosed() throws Exception {
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.NO_EXIT)) {
            Options options = new Options.Builder().
                                            server(ts.getURI()).
                                            pingInterval(Duration.ofMillis(10)).
                                            maxPingsOut(5).
                                            maxReconnects(0).
                                            build();
            NatsConnection nc = (NatsConnection) Nats.connect(options);

            try {
                assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
            } finally {
                nc.close();
            }

            Future<Boolean> pong = nc.sendPing();

            assertFalse(pong.get(10,TimeUnit.MILLISECONDS));
        }
    }

    @Test
    public void testMaxPingsOut() throws Exception {
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.NO_EXIT)) {
            Options options = new Options.Builder().
                                            server(ts.getURI()).
                                            pingInterval(Duration.ofSeconds(10)). // Avoid auto pings
                                            maxPingsOut(2).
                                            maxReconnects(0).
                                            build();
            NatsConnection nc = (NatsConnection) Nats.connect(options);

            //noinspection TryFinallyCanBeTryWithResources
            try {
                assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                nc.sendPing();
                nc.sendPing();
                assertNull(nc.sendPing(), "No future returned when past max");
            } finally {
                nc.close();
            }
        }
    }

    @Test
    public void testFlushTimeout() {
        assertThrows(TimeoutException.class, () -> {
            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.NO_EXIT)) {
                Options options = new Options.Builder().
                                                server(ts.getURI()).
                                                maxReconnects(0).
                                                build();
                NatsConnection nc = (NatsConnection) Nats.connect(options);

                //noinspection TryFinallyCanBeTryWithResources
                try {
                    assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                    // fake server so flush will time out
                    nc.flush(Duration.ofMillis(50));
                } finally {
                    nc.close();
                }
            }
        });
    }

    @Test
    public void testFlushTimeoutDisconnected() {
        assertThrows(TimeoutException.class, () -> {
            ListenerForTesting listener = new ListenerForTesting();
            try (NatsTestServer ts = new NatsTestServer(false)) {
                Options options = new Options.Builder().connectionListener(listener).server(ts.getURI()).build();
                NatsConnection nc = (NatsConnection) Nats.connect(options);

                try {
                    assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                    nc.flush(Duration.ofSeconds(2));
                    listener.prepForStatusChange(Events.DISCONNECTED);
                    ts.close();
                    listener.waitForStatusChange(2, TimeUnit.SECONDS);
                    nc.flush(Duration.ofSeconds(2));
                } finally {
                    nc.close();
                    assertSame(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
                }
            }
        });
    }

    @Test
    public void testPingTimerThroughReconnect() throws Exception {
        ListenerForTesting listener = new ListenerForTesting();
        try (NatsTestServer ts = new NatsTestServer(false)) {
            try (NatsTestServer ts2 = new NatsTestServer()) {
                Options options = new Options.Builder()
                    .connectionListener(listener)
                    .server(ts.getURI())
                    .server(ts2.getURI())
                    .pingInterval(Duration.ofMillis(5))
                    .maxPingsOut(10000) // just don't want this to be what fails the test
                    .build();
                NatsConnection nc = (NatsConnection) Nats.connect(options);
                StatisticsCollector stats = nc.getStatisticsCollector();

                try {
                    assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                    try {
                        Thread.sleep(200); // should get 10+ pings
                    } catch (Exception exp)
                    {
                        //Ignore
                    }
                    long pings = stats.getPings();
                    assertTrue(pings > 10, "got pings");
                    listener.prepForStatusChange(Events.RECONNECTED);
                    ts.close();
                    listener.waitForStatusChange(5, TimeUnit.SECONDS);
                    pings = stats.getPings();
                    Thread.sleep(250); // should get more pings
                    assertTrue(stats.getPings() > pings, "more pings");
                    Thread.sleep(1000);
                } finally {
                    nc.close();
                    assertSame(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
                }
            }
        }
    }


    @Test
    public void testMessagesDelayPings() throws Exception, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server(ts.getURI()).
                                    pingInterval(Duration.ofMillis(200)).build();
            NatsConnection nc = (NatsConnection) Nats.connect(options);
            StatisticsCollector stats = nc.getStatisticsCollector();

            try {
                final CompletableFuture<Boolean> done = new CompletableFuture<>();
                assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");

                Dispatcher d = nc.createDispatcher((msg) -> {
                    if (msg.getSubject().equals("done")) {
                        done.complete(Boolean.TRUE);
                    }
                });

                d.subscribe("subject");
                d.subscribe("done");
                nc.flush(Duration.ofMillis(1000)); // wait for them to go through

                long b4 = stats.getPings();
                for (int i=0;i<10;i++) {
                    Thread.sleep(50);
                    nc.publish("subject", new byte[16]);
                }
                long after = stats.getPings();
                assertEquals(after, b4, "pings hidden");
                nc.publish("done", new byte[16]);
                nc.flush(Duration.ofMillis(1000)); // wait for them to go through
                done.get(500, TimeUnit.MILLISECONDS);

                // no more messages, pings should start to go through
                b4 = stats.getPings();
                Thread.sleep(500);
                after = stats.getPings();
                assertTrue(after > b4, "pings restarted");
            } finally {
                nc.close();
            }
        }
    }

    @Test
    public void testRtt() throws Exception {
        runInJsServer(nc -> {
            assertTrue(nc.RTT().toMillis() < 10);
            nc.close();
            assertThrows(IOException.class, nc::RTT);
        });
    }
}
