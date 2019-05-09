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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;
import io.nats.client.NatsServerProtocolMock;
import io.nats.client.NatsTestServer;
import io.nats.client.Options;
import io.nats.client.TestHandler;
import io.nats.client.ConnectionListener.Events;
import io.nats.client.NatsServerProtocolMock.ExitAt;

public class PingTests {
    @Test
    public void testHandlingPing() throws IOException, InterruptedException,ExecutionException {
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
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                assertTrue("Got pong.", gotPong.get().booleanValue());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testPingTimer() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server(ts.getURI()).pingInterval(Duration.ofMillis(5)).build();
            NatsConnection nc = (NatsConnection) Nats.connect(options);
            NatsStatistics stats = nc.getNatsStatistics();

            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                try {
                    Thread.sleep(200); // should get 10+ pings
                } catch (Exception exp)
                {
                    //Ignore
                }
                assertTrue("got pings", stats.getPings() > 10);
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
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
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
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

            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                nc.sendPing();
                nc.sendPing();
                assertNull("No future returned when past max", nc.sendPing());
            } finally {
                nc.close();
            }
        }
    }

    @Test(expected=TimeoutException.class)
    public void testFlushTimeout() throws Exception {
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.NO_EXIT)) {
            Options options = new Options.Builder().
                                            server(ts.getURI()).
                                            maxReconnects(0).
                                            build();
            NatsConnection nc = (NatsConnection) Nats.connect(options);

            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                // fake server so flush will timeout
                nc.flush(Duration.ofMillis(50));
            } finally {
                nc.close();
            }
        }
    }

    @Test(expected=TimeoutException.class)
    public void testFlushTimeoutDisconnected() throws Exception {
        TestHandler handler = new TestHandler();
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().connectionListener(handler).server(ts.getURI()).build();
            NatsConnection nc = (NatsConnection) Nats.connect(options);

            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                nc.flush(Duration.ofSeconds(2));
                handler.prepForStatusChange(Events.DISCONNECTED);
                ts.close();
                handler.waitForStatusChange(2, TimeUnit.SECONDS);
                nc.flush(Duration.ofSeconds(2));
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testPingTimerThroughReconnect() throws IOException, InterruptedException {
        TestHandler handler = new TestHandler();
        try (NatsTestServer ts = new NatsTestServer(false)) {
            try (NatsTestServer ts2 = new NatsTestServer()) {
                Options options = new Options.Builder().connectionListener(handler).
                                        server(ts.getURI()).
                                        server(ts2.getURI()).
                                        pingInterval(Duration.ofMillis(5)).build();
                NatsConnection nc = (NatsConnection) Nats.connect(options);
                NatsStatistics stats = nc.getNatsStatistics();

                try {
                    assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                    try {
                        Thread.sleep(200); // should get 10+ pings
                    } catch (Exception exp)
                    {
                        //Ignore
                    }
                    long pings = stats.getPings();
                    assertTrue("got pings", pings > 10);
                    handler.prepForStatusChange(Events.RECONNECTED);
                    ts.close();
                    handler.waitForStatusChange(5, TimeUnit.SECONDS);
                    pings = stats.getPings();
                    try {
                        Thread.sleep(200); // should get more pings
                    } catch (Exception exp)
                    {
                        //Ignore
                    }
                    assertTrue("more pings", stats.getPings() > pings);
                } finally {
                    nc.close();
                    assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
                }
            }
        }
    }


    @Test
    public void testMessagesDelayPings() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server(ts.getURI()).
                                    pingInterval(Duration.ofMillis(200)).build();
            NatsConnection nc = (NatsConnection) Nats.connect(options);
            NatsStatistics stats = nc.getNatsStatistics();

            try {
                final CompletableFuture<Boolean> done = new CompletableFuture<>();
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

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
                assertTrue("pings hidden", after == b4);
                nc.publish("done", new byte[16]);
                nc.flush(Duration.ofMillis(1000)); // wait for them to go through
                done.get(500, TimeUnit.MILLISECONDS);

                // no more messages, pings should start to go through
                b4 = stats.getPings();
                Thread.sleep(500);
                after = stats.getPings();
                assertTrue("pings restarted", after > b4);
            } finally {
                nc.close();
            }
        }
    }
}