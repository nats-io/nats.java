package io.nats.examples.natsIoDoc;

import io.nats.client.*;

import java.nio.charset.StandardCharsets;

public class LearnJetStreamAcknowledgmentWatchMaxDeliveries {
    public static void main(String[] args) throws InterruptedException {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            // NATS-DOC-START
            // Watch the max-deliveries advisory for this consumer. The server
            // publishes one of these whenever a message hits MaxDeliver and is
            // dropped, which is your signal to route it to a dead-letter flow.
            String advisory = "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.ORDERS.shipping";

            Dispatcher d = nc.createDispatcher(msg ->
                System.out.println("max-deliveries advisory: " +
                    new String(msg.getData(), StandardCharsets.UTF_8)));
            d.subscribe(advisory);

            // Keep the subscription open so advisories can arrive.
            Thread.sleep(60_000);
            // NATS-DOC-END
        }
        catch (InterruptedException e) {
            throw e;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
