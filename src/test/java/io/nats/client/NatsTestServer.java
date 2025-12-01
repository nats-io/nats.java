// Copyright 2015-2022 The NATS Authors
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

import io.nats.ConsoleOutput;
import io.nats.NatsRunnerUtils;
import io.nats.NatsServerRunner;

import java.io.IOException;
import java.util.logging.Level;

import static io.nats.client.utils.ResourceUtils.configResource;

public class NatsTestServer extends NatsServerRunner implements TestServer {

    static {
        NatsTestServer.quiet();
        NatsRunnerUtils.setDefaultConnectValidateTries(10);
        NatsRunnerUtils.setDefaultConnectValidateTimeout(200);
        NatsRunnerUtils.setDefaultOutputSupplier(ConsoleOutput::new);
    }

    public static void quiet() {
        NatsRunnerUtils.setDefaultOutputLevel(Level.WARNING);
    }

    public static void verbose() {
        NatsRunnerUtils.setDefaultOutputLevel(Level.ALL);
    }

    public static Builder configFileBuilder(String configFilePath) {
        return NatsServerRunner.builder().configFilePath(configResource(configFilePath));
    }

    public static NatsTestServer configFileServer(String configFilePath) throws IOException {
        return new NatsTestServer(configFileBuilder(configFilePath));
    }

    public static NatsTestServer configFileServer(String configFilePath, int port) throws IOException {
        return new NatsTestServer(configFileBuilder(configFilePath).port(port));
    }

    public static NatsTestServer configuredJsServer(String configFilePath) throws IOException {
        return new NatsTestServer(configFileBuilder(configFilePath).jetstream());
    }

    public NatsTestServer() throws IOException {
        this(builder());
    }

    public NatsTestServer(int port) throws IOException {
        this(builder().port(port));
    }

    public NatsTestServer(int port, boolean jetstream) throws IOException {
        this(builder().port(port).jetstream(jetstream));
    }

    public NatsTestServer(String configFilePath, String[] configInserts, int port) throws IOException {
        this(builder().configFilePath(configResource(configFilePath)).configInserts(configInserts).port(port));
    }

    public NatsTestServer(String[] customArgs) throws IOException {
        this(builder().customArgs(customArgs));
    }

    public NatsTestServer(String[] customArgs, int port) throws IOException {
        this(builder().customArgs(customArgs).port(port));
    }

    public NatsTestServer(int port, boolean jetstream, String configFilePath, String[] configInserts, String[] customArgs) throws IOException {
        this(builder().port(port).jetstream(jetstream).configFilePath(configResource(configFilePath)).configInserts(configInserts).customArgs(customArgs));
    }

    public NatsTestServer(Builder b) throws IOException {
        super(b);
    }

    public static int nextPort() throws IOException {
        return NatsRunnerUtils.nextPort();
    }

    public String getLocalhostUri(String schema) {
        return NatsRunnerUtils.getLocalhostUri(schema, getPort());
    }

    @Override
    public String getServerUri() {
        return NatsRunnerUtils.getNatsLocalhostUri(getPort());
    }

    public static String getLocalhostUri(int port) {
        return NatsRunnerUtils.getNatsLocalhostUri(port);
    }

    public static String getLocalhostUri(String schema, int port) {
        return NatsRunnerUtils.getLocalhostUri(schema, port);
    }

    public static String[] getLocalhostUris(String schema, NatsTestServer... servers) {
        String[] results = new String[servers.length];
        for (int x = 0; x < servers.length; x++) {
            results[x] = NatsRunnerUtils.getLocalhostUri(schema, servers[x].getPort());
        }
        return results;
    }
}
