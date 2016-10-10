/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class UnitTestUtilities {
    // final Object mu = new Object();
    static NATSServer defaultServer = null;
    Process authServerProcess = null;

    static synchronized NATSServer runDefaultServer() {
        return runDefaultServer(false);
    }

    static synchronized NATSServer runDefaultServer(boolean debug) {
        NATSServer ns = new NATSServer(debug);
        sleep(100, TimeUnit.MILLISECONDS);
        return ns;
    }

    static synchronized Connection newDefaultConnection() throws IOException, TimeoutException {
        return new ConnectionFactory().createConnection();
    }

    static synchronized Connection newDefaultConnection(TCPConnectionFactory tcf)
            throws IOException, TimeoutException {
        return new ConnectionFactory().createConnection(tcf);
    }

    static synchronized Connection newDefaultConnection(TCPConnectionFactory tcf, Options opts)
            throws IOException, TimeoutException {
        ConnectionFactory cf = new ConnectionFactory();
        ConnectionImpl conn = new ConnectionImpl(opts != null ? opts : cf.options(), tcf);
        conn.connect();
        return conn;
    }

    static synchronized Connection newMockedConnection() throws IOException, TimeoutException {
        return newMockedConnection(null, null);
    }

    static synchronized Connection newMockedConnection(TCPConnectionFactoryMock tcf)
            throws IOException, TimeoutException {
        return newMockedConnection(tcf, null);
    }

    static synchronized Connection newMockedConnection(Options opts)
            throws IOException, TimeoutException {
        return newMockedConnection(null, opts);
    }

    static synchronized Connection newMockedConnection(TCPConnectionFactoryMock tcf, Options opts)
            throws IOException, TimeoutException {
        TCPConnectionFactory tcpConnFactory = null;
        if (tcf != null) {
            tcpConnFactory = null;
        } else {
            tcpConnFactory = new TCPConnectionFactoryMock();
        }
        return newDefaultConnection(tcpConnFactory,
                opts != null ? opts : new ConnectionFactory().options());
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

    static NATSServer runServerOnPort(int p) {
        return runServerOnPort(p, false);
    }

    static NATSServer runServerOnPort(int p, boolean debug) {
        NATSServer n = new NATSServer(p, debug);
        sleep(500);
        return n;
    }

    static NATSServer runServerWithConfig(String configFile) {
        return runServerWithConfig(configFile, false);
    }

    static NATSServer runServerWithConfig(String configFile, boolean debug) {
        NATSServer n = new NATSServer(configFile, debug);
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

            String currentLine = null; // store current line of output from the
                                       // cmd
            StringBuilder commandOutput = new StringBuilder(); // build up the
                                                               // output from
                                                               // cmd
            while ((currentLine = reader.readLine()) != null) {
                commandOutput.append(currentLine + "\n");
            }

            int returnCode = process.waitFor();
            if (returnCode == 0) {
                output = commandOutput.toString();
            }

        } catch (IOException e) {
            System.err.println("Cannot retrieve output of command");
            System.err.println(e);
            output = null;
        } catch (InterruptedException e) {
            System.err.println("Cannot retrieve output of command");
            System.err.println(e);
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
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(url.openStream(), "UTF-8"))) {
            for (String line; (line = reader.readLine()) != null;) {
                System.out.println(line);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
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
        }
        return val;
    }


    static synchronized void setLogLevel(ch.qos.logback.classic.Level level) {
        ch.qos.logback.classic.Logger lbLog =
                (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger("io.nats.client");
        lbLog.setLevel(level);
    }
}
