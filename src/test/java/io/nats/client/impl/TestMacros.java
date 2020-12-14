package io.nats.client.impl;

import io.nats.client.Connection;
import io.nats.client.TestHandler;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertSame;

public final class TestMacros {

    // ----------------------------------------------------------------------------------------------------
    // assertions
    // ----------------------------------------------------------------------------------------------------

    public static void assertConnected(Connection conn) {
        assertSame(Connection.Status.CONNECTED, conn.getStatus(), "Connected Status");
    }

    public static void assertClosed(Connection conn) {
        assertSame(Connection.Status.CLOSED, conn.getStatus(), "Closed Status");
    }

    // ----------------------------------------------------------------------------------------------------
    // utils
    // ----------------------------------------------------------------------------------------------------
    public static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { /* ignored */ }
    }

    // ----------------------------------------------------------------------------------------------------
    // macro utils
    // ----------------------------------------------------------------------------------------------------
    public static void allowTimeToConnectAssertOpen(Connection conn, TestHandler handler) {
        allowTimeToConnectAssertOpen(conn, handler, 5000);
    }

    public static void allowTimeToConnectAssertOpen(Connection conn, TestHandler handler, long millis) {
        handler.waitForStatusChange(millis, TimeUnit.MILLISECONDS);
        assertConnected(conn);
    }

    public static void closeConnectionAssertClosed(Connection conn) throws InterruptedException {
        if (conn != null) {
            conn.close();
            assertClosed(conn);
        }
    }
}
