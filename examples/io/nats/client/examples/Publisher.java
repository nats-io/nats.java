/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import io.nats.client.Statistics;

public class Publisher implements Runnable {
    static final Logger logger = LoggerFactory.getLogger(Publisher.class);

    Map<String, String> parsedArgs = new HashMap<String, String>();

    int count = 2000000;
    String url = ConnectionFactory.DEFAULT_URL;
    String subject = "foo";
    byte[] payload = null;

    Publisher(String[] args) {
        parseArgs(args);
        banner();
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void run() {
        long elapsed = 0L;
        try (Connection c = new ConnectionFactory(url).createConnection()) {

            long t0 = System.nanoTime();

            try {
                for (int i = 0; i < count; i++) {
                    c.publish(subject, payload);
                }
                c.flush();
            } catch (Exception e) {
                logger.error("Error during publish:", e);
            }

            long t1 = System.nanoTime();

            elapsed = TimeUnit.NANOSECONDS.toSeconds(t1 - t0);
            System.out.println("Elapsed time is " + elapsed + " seconds");

            System.out.printf("Published %d msgs in %d seconds ", count, elapsed);
            if (elapsed > 0) {
                System.out.printf("(%d msgs/second).\n", (int) (count / elapsed));
            } else {
                System.out.println("\nTest not long enough to produce meaningful stats. "
                        + "Please increase the message count (-count n)");
            }
            printStats(c);
        } catch (IOException | TimeoutException e) {
            logger.error("Couldn't connect:", e);
            System.exit(-1);
        }
    }

    private void printStats(Connection conn) {
        Statistics stats = conn.getStats();
        System.out.println("Statistics:  ");
        System.out.printf("   Outgoing Payload Bytes: %d\n", stats.getOutBytes());
        System.out.printf("   Outgoing Messages: %d\n", stats.getOutMsgs());
    }

    private void usage() {
        logger.error("Usage:  java Publish [-url url] [-subject subject] "
                + "-count [count] [-payload payload]");

        System.exit(-1);
    }

    private void parseArgs(String[] args) {
        if (args == null) {
            return;
        }

        for (int i = 0; i < args.length; i++) {
            if (i + 1 == args.length) {
                usage();
            }

            parsedArgs.put(args[i], args[i + 1]);
            i++;
        }

        if (parsedArgs.containsKey("-count")) {
            count = Integer.parseInt(parsedArgs.get("-count"));
        }

        if (parsedArgs.containsKey("-url")) {
            url = parsedArgs.get("-url");
        }

        if (parsedArgs.containsKey("-subject")) {
            subject = parsedArgs.get("-subject");
        }

        if (parsedArgs.containsKey("-payload")) {
            payload = parsedArgs.get("-payload").getBytes(Charset.forName("US-ASCII"));
        }
    }

    private void banner() {
        System.out.printf("Publishing %d messages on subject %s\n", count, subject);
        System.out.printf("  URL: %s\n", url);
        System.out.printf("  Payload is %d bytes.\n", payload != null ? payload.length : 0);
    }

    /**
     * Runs Publisher.
     * 
     * @param args the command line args
     */
    public static void main(String[] args) {
        try {
            new Publisher(args).run();
        } catch (Exception ex) {
            System.err.println("Exception: " + ex.getMessage());
            System.err.println(ex);
        } finally {
            System.exit(0);
        }


    }

}
