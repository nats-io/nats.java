/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.examples;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.nats.client.AsyncSubscription;
import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.Statistics;
import io.nats.client.SyncSubscription;

public class Replier implements Runnable {
    Map<String, String> parsedArgs = new HashMap<String, String>();

    int count = 20000;
    String url = ConnectionFactory.DEFAULT_URL;
    String subject = "foo";
    boolean sync = false;
    int received = 0;
    boolean verbose = false;

    long start = 0L;
    long end = 0L;

    Lock testLock = new ReentrantLock();
    Condition allDone = testLock.newCondition();
    boolean done = false;

    byte[] replyBytes = "reply".getBytes(Charset.forName("UTF-8"));

    Replier(String[] args) {
        parseArgs(args);
        banner();
    }

    @Override
    public void run() {
        try (Connection c = new ConnectionFactory(url).createConnection()) {
            long elapsed;
            long seconds;

            if (sync) {
                elapsed = receiveSyncSubscriber(c);
            } else {
                elapsed = receiveAsyncSubscriber(c);
            }
            seconds = TimeUnit.NANOSECONDS.toSeconds(elapsed);
            System.out.printf("Replied to %d msgs in %d seconds ", received, seconds);

            if (seconds > 0) {
                System.out.printf("(%d msgs/second).\n", (received / seconds));
            } else {
                System.out.println();
                System.out.println("Test not long enough to produce meaningful stats. "
                        + "Please increase the message count (-count n)");
            }
            printStats(c);
        } catch (IOException | TimeoutException e) {
            System.err.println("Couldn't connect: " + e.getMessage());
            System.exit(-1);
        }
    }

    private void printStats(Connection c) {
        Statistics s = c.getStats();
        System.out.printf("Statistics:  \n");
        System.out.printf("   Incoming Payload Bytes: %d\n", s.getInBytes());
        System.out.printf("   Incoming Messages: %d\n", s.getInMsgs());
        System.out.printf("   Outgoing Payload Bytes: %d\n", s.getOutBytes());
        System.out.printf("   Outgoing Messages: %d\n", s.getOutMsgs());
    }


    private long receiveAsyncSubscriber(Connection conn) {
        final Connection c = conn;
        MessageHandler mh = new MessageHandler() {
            public void onMessage(Message msg) {
                if (received++ == 0) {
                    start = System.nanoTime();
                }

                if (verbose) {
                    System.out.println("Received: " + msg);
                }

                try {
                    c.publish(msg.getReplyTo(), replyBytes);
                } catch (IllegalStateException | IOException e) {
                    e.printStackTrace();
                }

                if (received >= count) {
                    end = System.nanoTime();
                    testLock.lock();
                    try {
                        done = true;
                        allDone.signal();
                    } finally {
                        testLock.unlock();
                    }
                }
            }
        };

        AsyncSubscription s = c.subscribeAsync(subject, mh);
        // just wait to complete
        testLock.lock();
        try {
            s.start();
            while (!done) {
                allDone.await();
            }
        } catch (InterruptedException e) {
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            testLock.unlock();
        }

        return end - start;
    }


    private long receiveSyncSubscriber(Connection c) {
        SyncSubscription sub = c.subscribeSync(subject);

        Message m = null;
        try {
            while (received < count) {
                m = sub.nextMessage();
                if (received++ == 0) {
                    start = System.nanoTime();
                }

                if (verbose) {
                    System.out.println("Received: " + m);
                }

                c.publish(m.getReplyTo(), replyBytes);
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        end = System.nanoTime();
        return end - start;
    }

    private void usage() {
        System.err.println("Usage:  Publish [-url url] [-subject subject] "
                + "[-count count] [-sync] [-verbose]");

        System.exit(-1);
    }

    private void parseArgs(String[] args) {
        if (args == null) {
            return;
        }

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-sync") || args[i].equals("-verbose")) {
                parsedArgs.put(args[i], "true");
            } else {
                if (i + 1 == args.length) {
                    usage();
                }

                parsedArgs.put(args[i], args[i + 1]);
                i++;
            }

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

        if (parsedArgs.containsKey("-sync")) {
            sync = true;
        }

        if (parsedArgs.containsKey("-verbose")) {
            verbose = true;
        }
    }

    private void banner() {
        System.out.printf("Receiving %d messages on subject %s\n", count, subject);
        System.out.printf("  URL: %s\n", url);
        System.out.printf("  Receiving: %s\n", sync ? "Synchronously" : "Asynchronously");
    }

    public static void main(String[] args) {
        try {
            new Replier(args).run();
        } catch (Exception ex) {
            System.err.println("Exception: " + ex.getMessage());
            ex.printStackTrace();
        } finally {
            System.exit(0);
        }
    }



}
