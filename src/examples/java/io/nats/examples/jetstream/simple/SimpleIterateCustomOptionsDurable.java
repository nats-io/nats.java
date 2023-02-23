package io.nats.examples.jetstream.simple;

import io.nats.client.*;

import java.time.Duration;

import static io.nats.examples.jetstream.simple.SimpleUtils.*;

/**
 * This example will demonstrate simplified consuming using iterate
 */
public class SimpleIterateCustomOptionsDurable {

    public static void main(String[] args) {
        try (Connection nc = Nats.connect()) {

            setupStreamAndDataAndConsumer(nc);

            // JetStream context
            JetStream js = nc.jetStream();

            // Consumer[Context]
            ConsumerContext consumerContext = js.getConsumerContext(STREAM, DURABLE);

            // We want custom consume options
            ConsumeOptions co = ConsumeOptions.builder()
                .expiresIn(Duration.ofSeconds(3))
                .build();

            // create and use the iterator
            MessageNextConsumer consumer = consumerContext.fetch(COUNT * 2, co);

            int rec = 0;
            Message msg;
            while ((msg = consumer.nextMessage(1000)) != null) {
                System.out.printf("Subject: %s | Data: %s | Meta: %s\n",
                    msg.getSubject(), new String(msg.getData()), msg.getReplyTo());
                msg.ack();
                if (++rec == COUNT) {
                    System.out.println("\n*** Batch is larger than messages." +
                        "\n*** Custom ConsumeOption \"expires in\" is 3 seconds." +
                        "\n*** Wait for it to expire...");
                }
            }

            System.out.println("\n" + rec + " message(s) were received.\n");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
