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

import java.io.IOException;

import org.junit.Test;

public class ConnectTests {
    @Test
    public void testDefaultConnection() throws IOException {
        try (NatsTestServer ts = new NatsTestServer(Options.DEFAULT_PORT, true)) {
            Connection  nc = Nats.connect();
            nc.close();
        }
    }
    
    @Test
    public void testConnection() throws IOException {
        try (NatsTestServer ts = new NatsTestServer(true)) {
            Connection nc = Nats.connect("nats://localhost:"+ts.getPort());
            nc.close();
        }
    }
    
    @Test
    public void testConnectionWithOptions() throws IOException {
        try (NatsTestServer ts = new NatsTestServer(true)) {
            Options options = new Options.Builder().server("nats://localhost:"+ts.getPort()).build();
            Connection nc = Nats.connect(options);
            nc.close();
        }
    }
}