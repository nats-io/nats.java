package io.nats.examples.jetstream.simple;

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;

import java.io.IOException;

/**
 * This example will demonstrate simplified fetch
 */
public class SimpleFetch {
    private static final String STREAM = "simple-stream";
    private static final String SUBJECT = "simple-subject";
    private static final String CONSUMER_NAME_PREFIX = "simple-consumer";
    private static final int MESSAGES = 20;
    private static final int EXPIRES_SECONDS = 2;

    // change this is you need to...
    public static String SERVER = "nats://localhost:4222";

    public static void main(String[] args) {
        Options options = Options.builder().server(SERVER).build();
        try (Connection nc = Nats.connect(options)) {

            SimpleUtils.setupStreamAndData(nc, STREAM, SUBJECT, 20);

            // 1. Different fetch sizes demonstrate expiration behavior

            // 1A. equal number of messages than the fetch size
            simpleFetch(nc, 20, 0);

            // 1B. more messages than the fetch size
            simpleFetch(nc, 10, 0);

            // 1C. fewer messages than the fetch size
            simpleFetch(nc, 40, 0);

            // 2. Different max bytes sizes demonstrate expiration behavior
            //    - each test message is approximately 100 bytes

            // 2A. max bytes is reached before message count
            simpleFetch(nc, 20, 750);

            // 2B. fetch size is reached before byte count
            simpleFetch(nc, 10, 1500);

            // 2C. fewer bytes than the byte count
            simpleFetch(nc, 40, 3000);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void simpleFetch(Connection nc, int maxMessages, int maxBytes) throws IOException, JetStreamApiException, InterruptedException {
        JetStreamManagement jsm = nc.jetStreamManagement();
        JetStream js = nc.jetStream();

        String name = generateConsumerName(maxMessages, maxBytes);

        // Pre define a consumer
        ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(name).build();
        jsm.addOrUpdateConsumer(STREAM, cc);

        // Consumer[Context]
        ConsumerContext consumerContext = js.getConsumerContext(STREAM, name);

        // Custom consume options
        FetchConsumeOptions fetchConsumeOptions = FetchConsumeOptions.builder()
            .maxMessages(maxMessages)        // usually you would use only one or the other
            .maxBytes(maxBytes, maxMessages) // /\                                    /\
            .expiresIn(EXPIRES_SECONDS * 1000)
            .build();

        printExplanation(name, maxMessages, maxBytes);

        long start = System.currentTimeMillis();

        // create the consumer then use it
        FetchConsumer consumer = consumerContext.fetch(fetchConsumeOptions);
        int rcvd = 0;
        Message msg = consumer.nextMessage();
        while (msg != null) {
            ++rcvd;
            msg.ack();
            msg = consumer.nextMessage();
        }
        long elapsed = System.currentTimeMillis() - start;

        printSummary(rcvd, elapsed);
    }

    private static String generateConsumerName(int maxMessages, int maxBytes) {
        return maxBytes == 0
            ? CONSUMER_NAME_PREFIX + "-" + maxMessages + "msgs"
            : CONSUMER_NAME_PREFIX + "-" + maxBytes + "bytes-" + maxMessages + "msgs";
    }

    private static void printSummary(int rcvd, long elapsed) {
        System.out.println("+++ " + rcvd + " message(s) were received in " + elapsed + "ms\n");
    }

    private static void printExplanation(String name, int maxMessages, int maxBytes) {
        System.out.println("--------------------------------------------------------------------------------");
        System.out.println(name);
        if (maxBytes > 0) {
            if (maxMessages > MESSAGES) {
                System.out.println("=== Max bytes (" + maxBytes + ") is larger than available bytes.");
                System.out.println("=== FetchConsumeOption \"expires in\" is " + EXPIRES_SECONDS + " seconds.");
                System.out.println("=== nextMessage() blocks until expiration when there are no messages, then returns null.");
            }
            else if (maxMessages * 100 > maxBytes) {
                System.out.println("=== Max bytes (" + maxBytes + ") will be reached before fetch size (" + MESSAGES + ")");
                System.out.println("=== nextMessage() will return null when consume is done");
            }
            else {
                System.out.println("=== Fetch count (" + maxMessages + ") will be reached before max bytes (" + maxBytes + ")");
                System.out.println("=== nextMessage() will return null when consume is done");
            }
        }
        else if (maxMessages > MESSAGES) {
            System.out.println("=== Fetch (" + maxMessages + ") is larger than available messages (" + MESSAGES + ")");
            System.out.println("=== FetchConsumeOption \"expires in\" is " + EXPIRES_SECONDS + " seconds.");
            System.out.println("=== nextMessage() blocks until expiration when there are no messages, then returns null.");
        }
        else {
            System.out.println("=== Fetch (" + maxMessages + ") is less than or equal to available messages (" + MESSAGES + ")");
            System.out.println("=== nextMessage() will return null when consume is done");
        }
    }
}
