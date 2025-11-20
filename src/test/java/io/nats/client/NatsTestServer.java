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

public class NatsTestServer extends NatsServerRunner {

    private static final String CONFIG_FILE_BASE = "src/test/resources/";

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

    public static String configFilePath(String configFilePath) {
        return configFilePath.startsWith(CONFIG_FILE_BASE) ? configFilePath : CONFIG_FILE_BASE + configFilePath;
    }

    public static NatsTestServer configuredServer(String configFilePath) throws IOException {
        return new NatsTestServer(
            NatsServerRunner.builder()
                .configFilePath(configFilePath(configFilePath)));
    }

    public static NatsTestServer configuredJsServer(String configFilePath) throws IOException {
        return new NatsTestServer(
            NatsServerRunner.builder()
                .jetstream(true)
                .configFilePath(configFilePath(configFilePath)));
    }

    public static NatsTestServer configuredServer(String configFilePath, int port) throws IOException {
        return new NatsTestServer(
            NatsServerRunner.builder()
                .port(port)
                .configFilePath(configFilePath(configFilePath)));
    }

    public static NatsTestServer skipValidateServer(String configFilePath) throws IOException {
        return new NatsTestServer(
            NatsServerRunner.builder()
                .configFilePath(configFilePath(configFilePath))
                .skipConnectValidate());
    }

    public NatsTestServer() throws IOException {
        this(builder());
    }

    public NatsTestServer(boolean debug) throws IOException {
        this(builder().debug(debug));
    }

    public NatsTestServer(boolean debug, boolean jetstream) throws IOException {
        this(builder().debug(debug).jetstream(jetstream));
    }

    public NatsTestServer(int port, boolean debug) throws IOException {
        this(builder().port(port).debug(debug));
    }

    public NatsTestServer(int port, boolean debug, boolean jetstream) throws IOException {
        this(builder().port(port).debug(debug).jetstream(jetstream));
    }

    public NatsTestServer(String configFilePath, boolean debug) throws IOException {
        this(builder().configFilePath(configFilePath).debug(debug));
    }

    public NatsTestServer(String configFilePath, boolean debug, boolean jetstream) throws IOException {
        this(builder().configFilePath(configFilePath).debug(debug).jetstream(jetstream));
    }

    public NatsTestServer(String configFilePath, String[] configInserts, int port, boolean debug) throws IOException {
        this(builder().configFilePath(configFilePath).configInserts(configInserts).debug(debug));
    }

    public NatsTestServer(String configFilePath, int port, boolean debug) throws IOException {
        this(builder().configFilePath(configFilePath).port(port).debug(debug));
    }

    public NatsTestServer(String[] customArgs, boolean debug) throws IOException {
        this(builder().customArgs(customArgs).debug(debug));
    }

    public NatsTestServer(String[] customArgs, int port, boolean debug) throws IOException {
        this(builder().customArgs(customArgs).port(port).debug(debug));
    }

    public NatsTestServer(int port, boolean debug, boolean jetstream, String configFilePath, String[] configInserts, String[] customArgs) throws IOException {
        this(builder().port(port).debug(debug).jetstream(jetstream).configFilePath(configFilePath).configInserts(configInserts).customArgs(customArgs));
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

    public String getNatsLocalhostUri() {
        return NatsRunnerUtils.getNatsLocalhostUri(getPort());
    }

    public static String getNatsLocalhostUri(int port) {
        return NatsRunnerUtils.getNatsLocalhostUri(port);
    }

    public static String getLocalhostUri(String schema, int port) {
        return NatsRunnerUtils.getLocalhostUri(schema, port);
    }
}
