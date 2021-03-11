// Copyright 2015-2020 The NATS Authors
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

import nats.io.NatsRunnerUtils;
import nats.io.NatsServerRunner;

import java.io.IOException;

public class NatsTestServer extends NatsServerRunner {
    public NatsTestServer() throws IOException {
        super();
    }

    public NatsTestServer(boolean debug) throws IOException {
        super(debug);
    }

    public NatsTestServer(boolean debug, boolean jetstream) throws IOException {
        super(debug, jetstream);
    }

    public NatsTestServer(int port, boolean debug) throws IOException {
        super(port, debug);
    }

    public NatsTestServer(int port, boolean debug, boolean jetstream) throws IOException {
        super(port, debug, jetstream);
    }

    public NatsTestServer(String configFilePath, boolean debug) throws IOException {
        super(configFilePath, debug);
    }

    public NatsTestServer(String configFilePath, boolean debug, boolean jetstream) throws IOException {
        super(configFilePath, debug, jetstream);
    }

    public NatsTestServer(String configFilePath, String[] configInserts, int port, boolean debug) throws IOException {
        super(configFilePath, configInserts, port, debug);
    }

    public NatsTestServer(String configFilePath, int port, boolean debug) throws IOException {
        super(configFilePath, port, debug);
    }

    public NatsTestServer(String[] customArgs, boolean debug) throws IOException {
        super(customArgs, debug);
    }

    public NatsTestServer(String[] customArgs, int port, boolean debug) throws IOException {
        super(customArgs, port, debug);
    }

    public NatsTestServer(int port, boolean debug, boolean jetstream, String configFilePath, String[] configInserts, String[] customArgs) throws IOException {
        super(port, debug, jetstream, configFilePath, configInserts, customArgs);
    }

    public static int nextPort() throws IOException {
        return NatsRunnerUtils.nextPort();
    }

    public String getURI(String schema) {
        return getURIForPort(schema, getPort());
    }

    public static String getURIForPort(String schema, int port) {
        return schema + "://localhost:" + port;
    }
}