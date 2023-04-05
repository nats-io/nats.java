package io.nats.examples.jetstream.simple;

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;

import java.io.IOException;

/**
 * This example will demonstrate simplified consuming using
 * - fetch (non blocking)
 * - ConsumeOptions
 *   - custom message count
 *   - custom byte count
 */
public class SimpleFetch {
    public static String SERVER = "nats://localhost:4222";
    public static String STREAM = "simple-stream";
    public static String SUBJECT = "simple-subject";
    public static String NAME_PREFIX = "simple-consumer";
    public static int MESSAGE_COUNT = 20;

    public static void main(String[] args) {
        SimpleErrorListener el = new SimpleErrorListener();
        Options options = Options.builder().server(SERVER).errorListener(el).build();
        try (Connection nc = Nats.connect(options)) {

            SimpleUtils.setupStreamAndData(nc, STREAM, SUBJECT, MESSAGE_COUNT);

            // 1. Different batch sizes demonstrate expiration behavior

            // 1A. equal number of messages than the batch (fetch) size
            simpleFetch(nc, MESSAGE_COUNT, 0, el);

            // 1B. more messages than the batch (fetch) size
            simpleFetch(nc, MESSAGE_COUNT / 2, 0, el);

            // 1C. fewer messages than the batch (fetch) size
            simpleFetch(nc, MESSAGE_COUNT * 2, 0, el);

            // 2. Different max bytes sizes demonstrate expiration behavior
            //    - each test message is approximately 100 bytes

            // 2A. max bytes is reached before message count
            simpleFetch(nc, MESSAGE_COUNT, 750, el);

            // 2B. batch size is reached before byte count
            simpleFetch(nc, MESSAGE_COUNT / 2, 1500, el);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void simpleFetch(Connection nc, int batchSize, int maxBytes, SimpleErrorListener el) throws IOException, JetStreamApiException, InterruptedException {
        el.reset();
        JetStreamManagement jsm = nc.jetStreamManagement();
        JetStream js = nc.jetStream();

        String name = maxBytes == 0
            ? NAME_PREFIX + "-" + batchSize + "msgs"
            : NAME_PREFIX + "-" + maxBytes + "bytes-" + batchSize + "msgs";

        System.out.println("--------------------------------------------------------------------------------");
        System.out.println(name);

        // Pre define a consumer
        ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(name).build();
        jsm.addOrUpdateConsumer(STREAM, cc);

        // Consumer[Context]
        ConsumerContext consumerContext = js.getConsumerContext(STREAM, name);

        // Custom consume options
        ConsumeOptions consumeOptions = ConsumeOptions.builder()
            .batchSize(batchSize)
            .maxBytes(maxBytes)
            .expiresIn(3000)
            .build();

        // create and use the iterator
        boolean hasWarns = false;
        if (maxBytes > 0) {
            if (batchSize * 100 > maxBytes) {
                System.out.println("=== Max bytes (" + maxBytes + ") will be reached before batch size (" + MESSAGE_COUNT + ")");
                System.out.println("=== nextMessage() will return null when consume is done");
                System.out.println("=== Error Listener will receive an informational Pull Status Warning");
                hasWarns = true;
            }
            else {
                System.out.println("=== Batch count (" + batchSize + ") will be reached before max bytes (" + maxBytes + ")");
                System.out.println("=== nextMessage() will return null when consume is done");
            }
        }
        else if (batchSize > MESSAGE_COUNT) {
            System.out.println("=== Batch (" + batchSize + ") is larger than available messages (" + MESSAGE_COUNT + ")");
            System.out.println("=== ConsumeOption \"expires in\" is 3 seconds.");
            System.out.println("=== nextMessage() blocks until expiration when there are no messages.");
            System.out.println("=== null indicates consume is done");
        }
        else {
            System.out.println("=== Batch (" + batchSize + ") is less than or equal to available messages (" + MESSAGE_COUNT + ")");
            System.out.println("=== nextMessage() will return null when consume is done");
        }

        long start = System.currentTimeMillis();

        // create the consumer then use it
        FetchConsumer consumer = consumerContext.fetch(consumeOptions);
        int rcvd = 0;
        Message msg = consumer.nextMessage();
        while (msg != null) {
            ++rcvd;
            msg.ack();
            msg = consumer.nextMessage();
        }
        long elapsed = System.currentTimeMillis() - start;

        System.out.println("+++ " + rcvd + " message(s) were received in " + elapsed + "ms");
        if (hasWarns) {
            Thread.sleep(100); // just make sure there was time for the message to get to the listener
            System.out.println("!!! " + "Listener received warning: " + el.pullStatusWarning);
        }
        System.out.println();
    }
}
