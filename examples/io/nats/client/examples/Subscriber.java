package io.nats.client.examples;

import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.Subscription;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class Subscriber {
    static final Logger log = LoggerFactory.getLogger(Subscriber.class);

    private String url = ConnectionFactory.DEFAULT_URL;
    private String subject;
    private String qgroup;

    static final String usageString = "\nUsage: java Subscriber <subject>\n\nOptions:\n"
            + "    -s, --server   <url>            NATS server URL(default: "
            + ConnectionFactory.DEFAULT_URL + ")\n"
            + "    -q, --qgroup   <name>           Queue group\n";

    Subscriber(String[] args) {
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

        try (final Connection nc = cf.createConnection()) {
            // System.out.println("Connected successfully to " + cf.getNatsUrl());
            final AtomicInteger count = new AtomicInteger();
            try (final Subscription sub = nc.subscribe(subject, qgroup, new MessageHandler() {
                @Override
                public void onMessage(Message m) {
                    System.out.printf("[#%d] Received on [%s]: '%s'\n", count.incrementAndGet(),
                            m.getSubject(), m);
                }
            })) {
                System.out.printf("Listening on [%s]\n", subject);
                Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                    @Override
                    public void run() {
                        System.err.println("\nCaught CTRL-C, shutting down gracefully...\n");
                        try {
                            sub.unsubscribe();
                        } catch (IOException e) {
                            log.error("Problem unsubscribing", e);
                        }
                        nc.close();
                    }
                }));
                while (true) {
                    // loop forever
                }
            }
        }
    }

    private void parseArgs(String[] args) {
        if (args == null || args.length < 1) {
            usage();
            return;
        }

        List<String> argList = new ArrayList<String>(Arrays.asList(args));

        // The last arg should be subject
        // get the subject and remove it from args
        subject = argList.remove(argList.size() - 1);

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
                case "-q":
                case "--qgroup":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    qgroup = it.next();
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
     * Subscribes to a subject.
     * 
     * @param args the subject, cluster info, and subscription options
     */
    public static void main(String[] args) {
        try {
            new Subscriber(args).run();
        } catch (IOException | TimeoutException e) {
            log.error("Couldn't create Subscriber", e);
            System.exit(-1);
        }
        System.exit(0);
    }
}
