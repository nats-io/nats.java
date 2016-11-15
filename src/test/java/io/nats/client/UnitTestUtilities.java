/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

class UnitTestUtilities {
    // final Object mu = new Object();
    static NatsServer defaultServer = null;
    Process authServerProcess = null;

    static final org.slf4j.Logger logger = LoggerFactory.getLogger(UnitTestUtilities.class);

    static final String defaultInfo =
            "INFO {\"server_id\":\"a1c9cf0c66c3ea102c600200d441ad8e\",\"version\":\"0.7.2\",\"go\":"
                    + "\"go1.4.2\",\"host\":\"0.0.0.0\",\"port\":4222,\"auth_required\":false,"
                    + "\"ssl_required\":false,\"tls_required\":false,\"tls_verify\":false,"
                    + "\"max_payload\":1048576}";

    static final String defaultAsyncInfo =
            "INFO {\"server_id\":\"a1c9cf0c66c3ea102c600200d441ad8e\",\"version\":\"0.7.2\",\"go\":"
                    + "\"go1.4.2\",\"host\":\"0.0.0.0\",\"port\":4222,\"auth_required\":false,"
                    + "\"ssl_required\":false,\"tls_required\":false,\"tls_verify\":false,"
                    + "\"connect_urls\":[\"localhost:5222\"]," + "\"max_payload\":1048576}";

    static synchronized NatsServer runDefaultServer() {
        return runDefaultServer(false);
    }

    static synchronized NatsServer runDefaultServer(boolean debug) {
        NatsServer ns = new NatsServer(debug);
        sleep(100, TimeUnit.MILLISECONDS);
        return ns;
    }

    static synchronized Connection newDefaultConnection() throws IOException {
        return Nats.connect(Nats.DEFAULT_URL);
    }

    static synchronized Connection newDefaultConnection(TcpConnectionFactory tcf)
            throws IOException {
        return new Options.Builder().factory(tcf).build().connect();
    }

    static synchronized Connection newDefaultConnection(TcpConnectionFactory tcf, Options opts)
            throws IOException {
        return new Options.Builder(opts).factory(tcf).build().connect();
    }

    static Connection newMockedConnection() throws IOException {
        return newMockedConnection(null);
    }

    static TcpConnection newMockedTcpConnection() throws IOException {
        return newMockedTcpConnection(null);
    }

    static TcpConnection newMockedTcpConnection(ServerInfo info)
            throws IOException {
        // Set up mock TcpConnection
        final ServerInfo serverInfo =
                (info == null ? ServerInfo.createFromWire(defaultInfo) : info);

        final TcpConnection tcpConnMock = mock(TcpConnection.class);
        final BufferedReader bufferedReaderMock = mock(BufferedReader.class);
        final BufferedInputStream brMock = mock(BufferedInputStream.class);
        final BufferedOutputStream bwMock = mock(BufferedOutputStream.class);

        final byte[] pingBytes = ConnectionImpl.PING_PROTO.getBytes();
        final byte[] pongBytes = ConnectionImpl.PONG_PROTO.getBytes();

        final AtomicBoolean closed = new AtomicBoolean(false);
        final BlockingQueue<String> queue = new LinkedBlockingDeque<String>();
        // First lines are always INFO and then PONG
        logger.trace("\n\nSetting up new TCP Connection mock: {}", tcpConnMock);
        doReturn(serverInfo.toString(), new String(pongBytes).trim()).when(bufferedReaderMock)
                .readLine();

        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                setTcpConnState(tcpConnMock, true);
                return null;
            }
        }).when(tcpConnMock).open(any(String.class), anyInt());

        // Setup bufferedReaderMock
        doReturn(bufferedReaderMock).when(tcpConnMock).getBufferedReader();

        // Setup br
        doReturn(brMock).when(tcpConnMock).getInputStream(anyInt());

        // Setup br
        doReturn(bwMock).when(tcpConnMock).getOutputStream(anyInt());

        // Handle async pings
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                byte[] data = (byte[]) args[0];
                int offset = (int) args[1];
                int length = (int) args[2];
                queue.put(new String(data, offset, length));
                return null;
            }
        }).when(bwMock).write(any(byte[].class), anyInt(), anyInt());

        doAnswer(new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                // Answer a PING
                Object[] args = invocation.getArguments();
                byte[] buf = (byte[]) args[0];

                if (closed.get()) {
                    return -1;
                }

                String lastWrite = queue.poll(500, TimeUnit.MILLISECONDS);
                if (lastWrite != null && lastWrite.equals("PING")) {
                    System.arraycopy(pongBytes, 0, buf, 0, pongBytes.length);
                    return pongBytes.length;
                } else {
                    return 0;
                }
            }
        }).when(brMock).read(any(byte[].class));

        // Make sure connection resets on close
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                closed.set(true);
                setTcpConnState(tcpConnMock, false);
                return null;
            }
        }).when(tcpConnMock).close();

        return tcpConnMock;
    }

    static void setTcpConnState(TcpConnection mock, boolean open) {
        logger.trace("\n\nChanging state, connection ({}) = {}\n", mock, (open ? "open" :
                "closed"));
        logger.trace("old status, isClosed={}", mock.isClosed());
        doReturn(!open).when(mock).isClosed();
        logger.trace("new status, isClosed={}", mock.isClosed());
    }

    static TcpConnectionFactory newMockedTcpConnectionFactory() {
        return newMockedTcpConnectionFactory(ServerInfo.createFromWire(defaultInfo));
    }

    static TcpConnectionFactory newMockedTcpConnectionFactory(final ServerInfo info) {
        TcpConnectionFactory tcf = mock(TcpConnectionFactory.class);
        try {
            doAnswer(new Answer<TcpConnection>() {
                @Override
                public TcpConnection answer(InvocationOnMock invocationOnMock) throws Throwable {
                    TcpConnection conn = newMockedTcpConnection(info);
                    logger.trace("\n\nCreated new TcpConnection mock: {}\n", conn);
                    return conn;
                }
            }).when(tcf).createConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tcf;
    }

    static Connection newMockedConnection(Options opts)
            throws IOException {
        Options options = null;

        // Default in case not set in opts param
        TcpConnectionFactory tcfMock = newMockedTcpConnectionFactory();

        if (opts == null) {
            options = new Options.Builder(Nats.defaultOptions())
                    .factory(tcfMock)
                    .build();
            options.url = Nats.DEFAULT_URL;
        } else if (opts.getFactory() == null) {
            options = new Options.Builder(opts).factory(tcfMock).build();
        } else {
            options = new Options.Builder(opts).build();
        }

        return options.connect();
    }

    static synchronized void startDefaultServer() {
        startDefaultServer(false);
    }

    static synchronized void startDefaultServer(boolean debug) {
        if (defaultServer == null) {
            defaultServer = runDefaultServer(debug);
        }
    }

    static synchronized void stopDefaultServer() {
        if (defaultServer != null) {
            defaultServer.shutdown();
            defaultServer = null;
        }
    }

    static synchronized void bounceDefaultServer(int delayMillis) {
        stopDefaultServer();
        sleep(delayMillis);
        startDefaultServer();
    }

    public void startAuthServer() throws IOException {
        authServerProcess = Runtime.getRuntime().exec("gnatsd -config auth.conf");
    }

    static NatsServer runServerOnPort(int p) {
        return runServerOnPort(p, false);
    }

    static NatsServer runServerOnPort(int p, boolean debug) {
        NatsServer n = new NatsServer(p, debug);
        sleep(500);
        return n;
    }

    static NatsServer runServerWithConfig(String configFile) {
        return runServerWithConfig(configFile, false);
    }

    static NatsServer runServerWithConfig(String configFile, boolean debug) {
        NatsServer n = new NatsServer(configFile, debug);
        sleep(500);
        return n;
    }

    static String getCommandOutput(String command) {
        String output = null; // the string to return

        Process process = null;
        BufferedReader reader = null;
        InputStreamReader streamReader = null;
        InputStream stream = null;

        try {
            process = Runtime.getRuntime().exec(command);

            // Get stream of the console running the command
            stream = process.getInputStream();
            streamReader = new InputStreamReader(stream);
            reader = new BufferedReader(streamReader);

            // store current line of output from the cmd
            String currentLine = null;
            // build up the output from cmd
            StringBuilder commandOutput = new StringBuilder();
            while ((currentLine = reader.readLine()) != null) {
                commandOutput.append(currentLine).append("\n");
            }

            int returnCode = process.waitFor();
            if (returnCode == 0) {
                output = commandOutput.toString();
            }

        } catch (IOException e) {
            System.err.println("Cannot retrieve output of command");
            System.err.println(e.getMessage());
            output = null;
        } catch (InterruptedException e) {
            System.err.println("Cannot retrieve output of command");
            System.err.println(e.getMessage());
        } finally {
            // Close all inputs / readers

            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    System.err.println("Cannot close stream input! " + e);
                }
            }
            if (streamReader != null) {
                try {
                    streamReader.close();
                } catch (IOException e) {
                    System.err.println("Cannot close stream input reader! " + e);
                }
            }
            if (reader != null) {
                try {
                    streamReader.close();
                } catch (IOException e) {
                    System.err.println("Cannot close stream input reader! " + e);
                }
            }
        }
        // Return the output from the command - may be null if an error occured
        return output;
    }

    void getConnz() {
        URL url = null;
        try {
            url = new URL("http://localhost:8222/connz");
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

        try {
            if (url != null) {
                try (InputStream urlInputStream = url.openStream()) {
                    try (BufferedReader reader =
                                 new BufferedReader(new InputStreamReader(urlInputStream,
                                         "UTF-8"))) {
                        for (String line; (line = reader.readLine()) != null; ) {
                            System.out.println(line);
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void sleep(int timeout) {
        sleep(timeout, TimeUnit.MILLISECONDS);
    }

    static void sleep(int duration, TimeUnit unit) {
        try {
            unit.sleep(duration);
        } catch (InterruptedException e) {
            /* NOOP */
        }
    }

    static boolean await(CountDownLatch latch) {
        return await(latch, 5, TimeUnit.SECONDS);
    }

    static boolean await(CountDownLatch latch, long timeout, TimeUnit unit) {
        boolean val = false;
        try {
            val = latch.await(timeout, unit);
        } catch (InterruptedException e) {
            /* NOOP */
        }
        return val;
    }

    static synchronized void setLogLevel(ch.qos.logback.classic.Level level) {
        ch.qos.logback.classic.Logger lbLog =
                (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger("io.nats.client");
        lbLog.setLevel(level);
    }

    static void processServerConfigFile(String configFile) {

    }

    static StackTraceElement[] getStackTraceByName(String threadName) {
        Thread key = getThreadByName(threadName);
        return Thread.getAllStackTraces().get(key);
    }

    static Thread getThreadByName(String threadName) {
        Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
        for (Thread thd : threadSet) {
            if (thd.getName().equals(threadName)) {
                return thd;
            }
        }
        return null;
    }


}
