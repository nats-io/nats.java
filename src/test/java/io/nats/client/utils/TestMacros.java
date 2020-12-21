package io.nats.client.utils;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.TestHandler;
import org.opentest4j.AssertionFailedError;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

public final class TestMacros {

    private static final long STANDARD_WAIT = 5000;

    // ----------------------------------------------------------------------------------------------------
    // assertions
    // ----------------------------------------------------------------------------------------------------
    public static void assertConnected(Connection conn) {
        assertSame(Connection.Status.CONNECTED, conn.getStatus(),
                () -> expectingMessage(conn, Connection.Status.CONNECTED));
    }

    public static void assertNotConnected(Connection conn) {
        assertNotSame(Connection.Status.CONNECTED, conn.getStatus(),
                () -> "Failed not expecting Connection Status " + Connection.Status.CONNECTED.name());
    }

    public static void assertClosed(Connection conn) {
        assertSame(Connection.Status.CLOSED, conn.getStatus(),
                () -> expectingMessage(conn, Connection.Status.CLOSED));
    }

    public static void assertCanConnect() throws IOException, InterruptedException {
        standardCloseConnection( standardConnection() );
    }

    public static void assertCanConnect(String serverURL) throws IOException, InterruptedException {
        standardCloseConnection( standardConnection(serverURL) );
    }

    public static void assertCanConnect(Options options) throws IOException, InterruptedException {
        standardCloseConnection( standardConnection(options) );
    }

    // ----------------------------------------------------------------------------------------------------
    // utils / macro utils
    // ----------------------------------------------------------------------------------------------------
    public static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { /* ignored */ }
    }

    // ----------------------------------------------------------------------------------------------------
    // flush
    // ----------------------------------------------------------------------------------------------------
    public static void flushConnnection(Connection conn) {
        flushConnnection(conn, Duration.ofSeconds(1));
    }

    public static void flushConnection(Connection conn, int timeoutSeconds) {
        flushConnnection(conn, Duration.ofSeconds(timeoutSeconds));
    }

    public static void flushConnnection(Connection conn, Duration timeout) {
        try { conn.flush(timeout); } catch (Exception exp) { /* ignored */ }
    }

    // ----------------------------------------------------------------------------------------------------
    // connect or wait for a connection
    // ----------------------------------------------------------------------------------------------------

    public static Connection standardConnection() throws IOException, InterruptedException {
        return standardConnectionWait( Nats.connect() );
    }

    public static Connection standardConnection(String serverURL) throws IOException, InterruptedException {
        return standardConnectionWait( Nats.connect(serverURL) );
    }

    public static Connection standardConnection(Options options) throws IOException, InterruptedException {
        return standardConnectionWait( Nats.connect(options) );
    }

    public static Connection standardConnection(Options options, TestHandler handler) throws IOException, InterruptedException {
        return standardConnectionWait( Nats.connect(options), handler );
    }

    public static Connection standardConnectionWait(Connection conn, TestHandler handler) {
        return standardConnectionWait(conn, handler, STANDARD_WAIT);
    }

    public static Connection standardConnectionWait(Connection conn, TestHandler handler, long millis) {
        handler.waitForStatusChange(millis, TimeUnit.MILLISECONDS);
        assertConnected(conn);
        return conn;
    }

    public static Connection standardConnectionWait(Connection conn) {
        return connectionWait(conn, STANDARD_WAIT);
    }

    public static Connection connectionWait(Connection conn, long millis) {
        return waitUntilStatus(conn, millis, Connection.Status.CONNECTED);
    }

    // ----------------------------------------------------------------------------------------------------
    // close
    // ----------------------------------------------------------------------------------------------------
    public static void standardCloseConnection(Connection conn) {
        closeConnection(conn, STANDARD_WAIT);
    }

    public static void closeConnection(Connection conn, long millis) {
        if (conn != null) {
            close(conn);
            waitUntilStatus(conn, millis, Connection.Status.CLOSED);
            assertClosed(conn);
        }
    }

    public static void close(Connection conn) {
        try { conn.close(); } catch (InterruptedException e) { /* ignored */ }
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

    private static String expectingMessage(Connection conn, Connection.Status expecting) {
        return "Failed expecting Connection Status " + expecting.name() + " but was " + conn.getStatus();
    }
}
