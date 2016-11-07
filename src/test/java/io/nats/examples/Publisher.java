/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.examples;

import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import io.nats.client.Nats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class Publisher {
    String url = Nats.DEFAULT_URL;
    String subject;
    String payload;

    static final String usageString =
            "\nUsage: java Publisher [-s <server>] <subject> <message>\n\nOptions:\n"
                    + "    -s <url>        NATS server URLs, separated by commas\n";

    Publisher(String[] args) throws IOException, TimeoutException {
        parseArgs(args);
        if (subject == null) {
            usage();
        }
    }

    void usage() {
        System.err.println(usageString);
        System.exit(-1);
    }

    void run() throws IOException, TimeoutException {
        ConnectionFactory cf = new ConnectionFactory(url);
        try (Connection nc = cf.createConnection()) {
            nc.publish(subject, payload.getBytes());
            System.err.printf("Published [%s] : '%s'\n", subject, payload);
        }
    }

    private void parseArgs(String[] args) {
        if (args == null || args.length < 2) {
            usage();
            return;
        }

        List<String> argList = new ArrayList<String>(Arrays.asList(args));

        // The last arg should be subject and payload
        // get the payload and remove it from args
        payload = argList.remove(argList.size() - 1);

        // get the subject and remove it from args
        subject = argList.remove(argList.size() - 1);;

        // Anything left is flags + args
        Iterator<String> it = argList.iterator();
        while (it.hasNext()) {
            String arg = it.next();
            switch (arg) {
                case "-s":
                case "--server":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    url = it.next();
                    it.remove();
                    continue;
                default:
                    System.err.printf("Unexpected token: '%s'\n", arg);
                    usage();
                    break;
            }
        }
    }

    /**
     * Publishes a message to a subject.
     * 
     * @param args the subject, message payload, and other arguments
     */
    public static void main(String[] args) {
        try {
            new Publisher(args).run();
        } catch (IOException | TimeoutException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(-1);
        }
        System.exit(0);
    }
}
