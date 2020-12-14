package io.nats.client.impl;

import io.nats.client.Connection;
import io.nats.client.TestHandler;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;

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
        try {
            Thread.sleep(ms);
        }
        catch (InterruptedException e) {
            fail(e); // will never happen, but if it does...
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // macro utils
    // ----------------------------------------------------------------------------------------------------
    public static void waitThenAssertConnected(Connection conn, TestHandler handler) {
        waitThenAssertConnected(conn, handler, 5000);
    }

    public static void waitThenAssertConnected(Connection conn, TestHandler handler, long millis) {
        handler.waitForStatusChange(millis, TimeUnit.MILLISECONDS);
        assertConnected(conn);
    }

    public static void closeThenAssertClosed(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
                assertClosed(conn);
            } catch (InterruptedException e) {
                fail(e); // will never happen, but if it does...
            }
        }
    }
}
