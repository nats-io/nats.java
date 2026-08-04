package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Subscription;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class LearnCoreNatsScatterGatherGather {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {
            // Three shipping-quote providers, each answering on shipping.quote.
            for (int i = 0; i < 3; i++) {
                nc.createDispatcher(msg -> {
                    if (msg.getReplyTo() != null) {
                        nc.publish(msg.getReplyTo(),
                            "{\"carrier\":\"carrier-a\",\"quote_cents\":1500}".getBytes(StandardCharsets.UTF_8));
                    }
                }).subscribe("shipping.quote");
            }
            Thread.sleep(200);

            // NATS-DOC-START
            // Scatter one request to every shipping-quote provider and gather the
            // replies. Subscribe to a private inbox, publish the request with that
            // inbox as the reply subject, then collect quotes until they stop arriving
            // and pick the cheapest.
            byte[] order = ("{\"order_id\":\"ord_8w2k\",\"customer\":\"acme-co\","
                + "\"total_cents\":4200,\"ts\":\"2026-05-22T10:14:22Z\"}").getBytes(StandardCharsets.UTF_8);
            String inbox = nc.createInbox();
            Subscription sub = nc.subscribe(inbox);
            nc.publish("shipping.quote", inbox, order);

            List<String> quotes = new ArrayList<>();
            Message m = sub.nextMessage(Duration.ofMillis(300));
            while (m != null) {
                quotes.add(new String(m.getData(), StandardCharsets.UTF_8));
                m = sub.nextMessage(Duration.ofMillis(300));
            }
            System.out.println("gathered " + quotes.size() + " quotes");
            // NATS-DOC-END
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (IOException e) {
            // can be thrown by connect
        }
    }
}
