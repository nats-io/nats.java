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
import io.nats.client.Options;
import org.jspecify.annotations.Nullable;
import org.opentest4j.AssertionFailedError;

import java.io.IOException;

import static io.nats.client.utils.ThreadUtils.sleep;
import static org.junit.jupiter.api.Assertions.assertSame;

public abstract class ConnectionUtils {

    public static final long CONNECTION_WAIT_MS = 10000;
    public static final long STANDARD_FLUSH_TIMEOUT_MS = 2000;
    public static final long MEDIUM_FLUSH_TIMEOUT_MS = 5000;
    public static final long LONG_TIMEOUT_MS = 15000;

    // ----------------------------------------------------------------------------------------------------
    // standardConnect
    // ----------------------------------------------------------------------------------------------------
    private static final int RETRY_DELAY_INCREMENT = 50;
    private static final int CONNECTION_RETRIES = 10;
    private static final long RETRY_DELAY = 100;

    public static Connection standardConnect(Options options) {
        try {
            return standardConnect(options, null);
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to make a connection.", e);
        }
    }

    public static Connection standardConnect(Options options, @Nullable Class<?> throwImmediately) throws IOException, InterruptedException {
        IOException last = null;
        long delay = RETRY_DELAY - RETRY_DELAY_INCREMENT;
        for (int x = 1; x <= CONNECTION_RETRIES; x++) {
            if (x > 1) {
                delay += RETRY_DELAY_INCREMENT;
                sleep(delay);
            }
            try {
                return waitUntilConnected(Nats.connect(options));
            }
            catch (IOException ioe) {
                if (throwImmediately != null && throwImmediately.isAssignableFrom(ioe.getClass())) {
                    throw ioe;
                }
                last = ioe;
            }
            catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw ie;
            }
        }
        throw last;
    }

    // ----------------------------------------------------------------------------------------------------
    // connect or wait for a connection
    // ----------------------------------------------------------------------------------------------------
    public static Connection waitUntilConnected(Connection conn) {
        return waitUntilStatus(conn, CONNECTION_WAIT_MS, Connection.Status.CONNECTED);
    }

    // ----------------------------------------------------------------------------------------------------
    // close
    // ----------------------------------------------------------------------------------------------------
    public static void standardCloseConnection(Connection conn) {
        closeConnection(conn, CONNECTION_WAIT_MS);
    }

    public static void closeConnection(Connection conn, long millis) {
        if (conn != null) {
            close(conn);
            waitUntilStatus(conn, millis, Connection.Status.CLOSED);
            assertClosed(conn);
        }
    }

    public static void close(Connection conn) {
        try {
            conn.close();
        }
        catch (InterruptedException e) { /* ignored */ }
    }

    // ----------------------------------------------------------------------------------------------------
    // connection waiting
    // ----------------------------------------------------------------------------------------------------
    public static Connection waitUntilStatus(Connection conn, long millis, Connection.Status waitUntilStatus) {
        long times = (millis + 99) / 100;
        for (long x = 0; x < times; x++) {
            sleep(100);
            if (conn.getStatus() == waitUntilStatus) {
                return conn;
            }
        }

        throw new AssertionFailedError(expectingMessage(conn, waitUntilStatus));
    }

    // ----------------------------------------------------------------------------------------------------
    // assertions
    // ----------------------------------------------------------------------------------------------------
    public static void assertConnected(Connection conn) {
        assertSame(Connection.Status.CONNECTED, conn.getStatus(),
            () -> expectingMessage(conn, Connection.Status.CONNECTED));
    }

    public static void assertClosed(Connection conn) {
        assertSame(Connection.Status.CLOSED, conn.getStatus(),
            () -> expectingMessage(conn, Connection.Status.CLOSED));
    }

    public static void assertCanConnect(Options options) {
        standardCloseConnection(standardConnect(options));
    }

    private static String expectingMessage(Connection conn, Connection.Status expecting) {
        return "Failed expecting Connection Status " + expecting.name() + " but was " + conn.getStatus();
    }
}
