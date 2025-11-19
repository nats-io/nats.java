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

import io.nats.client.*;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

import static io.nats.client.utils.TestBase.VERY_LONG_CONNECTION_WAIT_MS;
import static io.nats.client.utils.TestBase.initRunServerInfo;

public abstract class LongRunningServer {

    private static final ErrorListener NO_OP_EL = new ErrorListener() {};

    private static final ReentrantLock lock = new ReentrantLock();
    private static final int MAX_TRIES = 3;

    private static NatsTestServer natsTestServer;
    private static String uri;
    private static Connection[] ncs;

    public static Options.Builder optionsBuilder() throws IOException {
        return new Options.Builder().server(uri()).errorListener(NO_OP_EL);
    }

    public static Options options() throws IOException {
        return optionsBuilder().build();
    }

    public static String uri() throws IOException {
        if (uri == null) {
            ensureInitialized();
        }
        return uri;
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
                ncs[ix] = TestBase.connectionWait(Nats.connect(optionsBuilder().build()), VERY_LONG_CONNECTION_WAIT_MS);
                initRunServerInfo(ncs[ix]);
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
                uri = natsTestServer.getURI();
//                System.out.println("LongRunningServer create " + uri);

                final Thread shutdownHookThread = new Thread("LongRunningServer-Shutdown-Hook") {
                    @Override
                    public void run() {
                        try {
//                            System.out.println("LongRunningServer shutting down");
                            natsTestServer.shutdown(false);
                        }
                        catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                };

                Runtime.getRuntime().addShutdownHook(shutdownHookThread);
            }
//            System.out.println("LongRunningServer return " + natsTestServer.getNatsLocalhostUri());
        }
        finally {
            lock.unlock();
        }
    }
}
