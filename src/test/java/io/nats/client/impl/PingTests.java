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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.NatsServerProtocolMock;
import io.nats.client.NatsTestServer;
import io.nats.client.Options;
import io.nats.client.NatsServerProtocolMock.ExitAt;
import io.nats.client.NatsServerProtocolMock.Progress;

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
            assertTrue("Progress", Progress.COMPLETED_CUSTOM_CODE == ts.getProgress());
        }
    }

    @Test
    public void testPingTimer() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server(ts.getURI()).pingInterval(Duration.ofMillis(10)).build();
            NatsConnection nc = (NatsConnection) Nats.connect(options);
            NatsStatistics stats = nc.getNatsStatistics();

            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                try {
                    Thread.sleep(120); // should get 10+ pings
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
    public void testMaxPings() throws IOException, InterruptedException {
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.NO_EXIT)) {
            Options options = new Options.Builder().
                                            server(ts.getURI()).
                                            pingInterval(Duration.ofMillis(10)).
                                            maxPingsOut(5).
                                            maxReconnects(0).
                                            build();
            NatsConnection nc = (NatsConnection) Nats.connect(options);
            NatsStatistics stats = nc.getNatsStatistics();

            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                try {
                    Thread.sleep(120); // should get 10+ pings
                } catch (Exception exp)
                {
                    //Ignore
                }
                // Should close because we exceeded and can't reconnect
                assertTrue("Connected Status", Connection.Status.CLOSED == nc.getStatus());
                assertTrue("got pings", stats.getPings() >= 1);
            } finally {
                nc.close();
            }
        }
    }
}