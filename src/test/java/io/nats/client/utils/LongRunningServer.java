// Copyright 2025 The NATS Authors
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

package io.nats.client.utils;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.NatsTestServer;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

import static io.nats.client.utils.ConnectionUtils.VERY_LONG_CONNECTION_WAIT_MS;
import static io.nats.client.utils.ConnectionUtils.connectionWait;
import static io.nats.client.utils.OptionsUtils.options;
import static io.nats.client.utils.VersionUtils.initVersionServerInfo;

public abstract class LongRunningServer {

    private static final ReentrantLock lock = new ReentrantLock();
    private static final int MAX_TRIES = 3;

    private static NatsTestServer natsTestServer;
    private static String server;
    private static Connection[] ncs;

    public static String server() throws IOException {
        if (server == null) {
            ensureInitialized();
        }
        return server;
    }

    // do not use LrConns in try-resources
    public static Connection getLrConn() throws IOException, InterruptedException {
        return _getLrConnection(0, 0);
    }

    public static Connection getLrConn2() throws IOException, InterruptedException {
        return _getLrConnection(1, 0);
    }

    private static Connection _getLrConnection(int ix, int tries) throws IOException, InterruptedException {
        lock.lock();
        try {
            if (ncs == null) {
                ncs = new Connection[2];
            }
            if (ncs[ix] == null) {
                ncs[ix] = connectionWait(Nats.connect(options(server())), VERY_LONG_CONNECTION_WAIT_MS);
                initVersionServerInfo(ncs[ix]);
            }
            else if (ncs[ix].getStatus() != Connection.Status.CONNECTED) {
                if (++tries < MAX_TRIES) {
                    Connection c = ncs[ix];
                    c.getOptions().getExecutor().execute(() -> {
                        try {
                            c.close();
                        }
                        catch (InterruptedException ignore) {
                        }
                    });
                    ncs[ix] = null;
                    return _getLrConnection(ix, tries);
                }
            }
            return ncs[ix];
        }
        finally {
            lock.unlock();
        }
    }

    private static void ensureInitialized() throws IOException {
        lock.lock();
        try {
            if (natsTestServer == null) {
                natsTestServer = new NatsTestServer(
                    NatsTestServer.builder()
                        .jetstream(true)
                        .customName("LongRunningServer")
                );
                server = natsTestServer.getLocalhostUri();

                final Thread shutdownHookThread = new Thread("LongRunningServer-Shutdown-Hook") {
                    @Override
                    public void run() {
                        try {
                            natsTestServer.shutdown(false);
                        }
                        catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                };

                Runtime.getRuntime().addShutdownHook(shutdownHookThread);
            }
        }
        finally {
            lock.unlock();
        }
    }
}
