package io.nats.examples.jetstream.simple;

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;

import java.io.IOException;

/**
 * This example will demonstrate simplified fetch
 */
public class FetchExample {
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

            Utils.setupStreamAndData(nc, STREAM, SUBJECT, 20);

            // 1. Different fetch max messages demonstrate expiration behavior

            // 1A. equal number of messages to the fetch max messages
            simpleFetch("1A", nc, 20, 0);

            // 1B. more messages than the fetch max messages
            simpleFetch("1B", nc, 10, 0);

            // 1C. fewer messages than the fetch max messages
            simpleFetch("1C", nc, 40, 0);

            // 2. Different max bytes sizes demonstrate expiration behavior
            //    - each test message is approximately 100 bytes

            // 2A. max bytes is reached before message count
            simpleFetch("2A", nc, 10, 750);

            // 2B. fetch max messages is reached before byte count
            simpleFetch("2B", nc, 10, 1500);

            // 2C. fewer bytes than the byte count
            simpleFetch("2C", nc, 25, 3000);

            // 3. simple-consumer-40msgs was created in 1C and has no messages available
            simpleFetch("3", nc, 40, 0);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void simpleFetch(String label, Connection nc, int maxMessages, int maxBytes) throws IOException, JetStreamApiException, InterruptedException {
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

        printExplanation(label, name, maxMessages, maxBytes);

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

    private static void printExplanation(String label, String name, int maxMessages, int maxBytes) {
        System.out.println("--------------------------------------------------------------------------------");
        System.out.println(label + ". " + name);
        switch (label) {
            case "1A":
            case "1B":
                System.out.println("=== Fetch (" + maxMessages + ") is less than or equal to available messages (" + MESSAGES + ")");
                System.out.println("=== nextMessage() will return null when consume is done");
                break;
            case "1C":
                System.out.println("=== Fetch (" + maxMessages + ") is larger than available messages (" + MESSAGES + ")");
                System.out.println("=== FetchConsumeOption \"expires in\" is " + EXPIRES_SECONDS + " seconds.");
                System.out.println("=== nextMessage() blocks until expiration when there are no messages available, then returns null.");
                break;
            case "2A":
                System.out.println("=== Max bytes (" + maxBytes + ") will be reached before fetch message count (" + MESSAGES + ")");
                System.out.println("=== nextMessage() will return null when consume is done");
                break;
            case "2B":
                System.out.println("=== Fetch count (" + maxMessages + ") will be reached before max bytes (" + maxBytes + ")");
                System.out.println("=== nextMessage() will return null when consume is done");
                break;
            case "2C":
                System.out.println("=== Max bytes (" + maxBytes + ") is larger than available bytes.");
                System.out.println("=== FetchConsumeOption \"expires in\" is " + EXPIRES_SECONDS + " seconds.");
                System.out.println("=== nextMessage() blocks until expiration when there are no messages available, then returns null.");
                break;
            case "3":
                System.out.println("=== Fetch (" + maxMessages + ") is larger than available messages (0)");
                System.out.println("=== FetchConsumeOption \"expires in\" is " + EXPIRES_SECONDS + " seconds.");
                System.out.println("=== nextMessage() blocks until expiration when there are no messages available, then returns null.");
                break;
        }
    }
}
