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
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.nats.client.utils.ConnectionUtils.standardConnectionWait;
import static io.nats.client.utils.OptionsUtils.optionsBuilder;
import static io.nats.client.utils.ThreadUtils.sleep;
import static org.junit.jupiter.api.Assertions.*;

public class PingTests extends TestBase {
    @Test
    public void testHandlingPing() throws Exception {
        CompletableFuture<Boolean> gotPong = new CompletableFuture<>();

        NatsServerProtocolMock.Customizer pingPongCustomizer = (ts, r,w) -> {
//            System.out.println("*** Mock Server @" + ts.getPort() + " sending PING ...");
            w.write("PING\r\n");
            w.flush();

            String pong;
//            System.out.println("*** Mock Server @" + ts.getPort() + " waiting for PONG ...");
            try {
                pong = r.readLine();
            } catch(Exception e) {
                gotPong.cancel(true);
                return;
            }

            if (pong.startsWith("PONG")) {
//                System.out.println("*** Mock Server @" + ts.getPort() + " got PONG ...");
                gotPong.complete(Boolean.TRUE);
            } else {
//                System.out.println("*** Mock Server @" + ts.getPort() + " got something else... " + pong);
                gotPong.complete(Boolean.FALSE);
            }
        };

        try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(pingPongCustomizer)) {
            Connection  nc = Nats.connect(mockTs.getMockUri());
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
        Options.Builder builder = optionsBuilder()
            .pingInterval(Duration.ofMillis(5))
            .maxPingsOut(10000); // just don't want this to be what fails the test
        runInSharedOwnNc(builder, nc -> {
            Statistics stats = nc.getStatistics();
            sleep(200); // 1200 / 100 ... should get 10+ pings
            assertTrue(stats.getPings() > 1, "got pings");
        });
    }

    @Test
    public void testPingFailsWhenClosed() throws Exception {
        try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(ExitAt.NO_EXIT)) {
            Options options = optionsBuilder().server(mockTs.getMockUri()).
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
        try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(ExitAt.NO_EXIT)) {
            Options options = optionsBuilder().
                                            server(mockTs.getMockUri()).
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
            try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(ExitAt.NO_EXIT)) {
                Options options = optionsBuilder().
                                                server(mockTs.getMockUri()).
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
    public void testFlushTimeoutDisconnected() throws Exception {
        ListenerForTesting listener = new ListenerForTesting();
        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = optionsBuilder(ts).connectionListener(listener).build();
            NatsConnection nc = (NatsConnection) Nats.connect(options);
            try {
                assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
                nc.flush(Duration.ofSeconds(2));
                listener.prepForStatusChange(Events.DISCONNECTED);
                ts.close();
                listener.waitForStatusChange(2, TimeUnit.SECONDS);
                assertThrows(TimeoutException.class, () -> nc.flush(Duration.ofSeconds(2)));
            }
            finally {
                nc.close();
                assertSame(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
            }
        }
    }

    @Test
    public void testPingTimerThroughReconnect() throws Exception {
        ListenerForTesting listener = new ListenerForTesting();
        try (NatsTestServer ts = new NatsTestServer()) {
            try (NatsTestServer ts2 = new NatsTestServer()) {
                Options options = optionsBuilder(ts.getLocalhostUri(), ts2.getLocalhostUri())
                    .connectionListener(listener)
                    .pingInterval(Duration.ofMillis(5))
                    .maxPingsOut(10000) // just don't want this to be what fails the test
                    .build();
                try (NatsConnection nc = (NatsConnection) standardConnectionWait(options)) {
                    StatisticsCollector stats = nc.getStatisticsCollector();
                    sleep(200);
                    long pings = stats.getPings();
                    assertTrue(pings > 10, "got pings");
                    listener.prepForStatusChange(Events.RECONNECTED);
                    ts.close();
                    listener.waitForStatusChange(5, TimeUnit.SECONDS);
                    pings = stats.getPings();
                    sleep(250); // should get more pings
                    assertTrue(stats.getPings() > pings, "more pings");
                    sleep(1000);
                }
            }
        }
    }
}
