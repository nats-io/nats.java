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

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import io.nats.client.FakeNatsTestServer.ExitAt;
import io.nats.client.FakeNatsTestServer.Progress;

public class ConnectTests {
    @Test
    public void testDefaultConnection() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(Options.DEFAULT_PORT, false)) {
            Connection  nc = Nats.connect();
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
        }
    }
    
    @Test
    public void testConnection() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Connection nc = Nats.connect("nats://localhost:"+ts.getPort());
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
        }
    }
    
    @Test
    public void testConnectionWithOptions() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server("nats://localhost:"+ts.getPort()).build();
            Connection nc = Nats.connect(options);
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
        }
    }

    @Test
    public void testFullFakeConnect() throws IOException, InterruptedException {
        try (FakeNatsTestServer ts = new FakeNatsTestServer(ExitAt.NO_EXIT)) {
            Connection  nc = Nats.connect("nats://localhost:"+ts.getPort());
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            assertTrue("Progress", Progress.SENT_PONG == ts.getProgress());
        }
    }

    @Test
    public void testConnectExitBeforeInfo() throws IOException, InterruptedException {
        try (FakeNatsTestServer ts = new FakeNatsTestServer(ExitAt.EXIT_BEFORE_INFO)) {
            Connection  nc = Nats.connect("nats://localhost:"+ts.getPort());
            assertTrue("Connected Status", Connection.Status.DISCONNECTED == nc.getStatus());
            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            assertTrue("Progress", Progress.CLIENT_CONNECTED == ts.getProgress());
        }
    }

    @Test
    public void testConnectExitAfterInfo() throws IOException, InterruptedException {
        try (FakeNatsTestServer ts = new FakeNatsTestServer(ExitAt.EXIT_AFTER_INFO)) {
            Connection  nc = Nats.connect("nats://localhost:"+ts.getPort());
            assertTrue("Connected Status", Connection.Status.DISCONNECTED == nc.getStatus());
            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            assertTrue("Progress", Progress.SENT_INFO == ts.getProgress());
        }
    }

    @Test
    public void testConnectExitAfterConnect() throws IOException, InterruptedException {
        try (FakeNatsTestServer ts = new FakeNatsTestServer(ExitAt.EXIT_AFTER_CONNECT)) {
            Connection  nc = Nats.connect("nats://localhost:"+ts.getPort());
            assertTrue("Connected Status", Connection.Status.DISCONNECTED == nc.getStatus());
            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            assertTrue("Progress", Progress.GOT_CONNECT == ts.getProgress());
        }
    }

    @Test
    public void testConnectExitAfterPing() throws IOException, InterruptedException {
        try (FakeNatsTestServer ts = new FakeNatsTestServer(ExitAt.EXIT_AFTER_PING)) {
            Connection  nc = Nats.connect("nats://localhost:"+ts.getPort());
            assertTrue("Connected Status", Connection.Status.DISCONNECTED == nc.getStatus());
            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            assertTrue("Progress", Progress.GOT_PING == ts.getProgress());
        }
    }
    
    @Test
    public void testConnectionFailureWithFallback() throws IOException, InterruptedException {
        try (FakeNatsTestServer fake = new FakeNatsTestServer(ExitAt.EXIT_AFTER_PING);
                NatsTestServer ts = new NatsTestServer(true)) {
            Options options = new Options.Builder().
                                server("nats://localhost:"+fake.getPort()).
                                server("nats://localhost:"+ts.getPort()).build();
            Connection nc = Nats.connect(options);
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            assertTrue("Progress", Progress.GOT_PING == fake.getProgress());
        }
    }
}