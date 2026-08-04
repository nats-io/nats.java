package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Nats;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class LearnCoreNatsScatterGatherProvider {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {
            // NATS-DOC-START
            // A shipping-quote provider. Subscribe plainly to shipping.quote (NOT in a
            // queue group, so every provider sees each request) and reply with a price.
            // Run several copies, each quoting a different number.
            nc.createDispatcher(msg -> {
                if (msg.getReplyTo() != null) {
                    nc.publish(msg.getReplyTo(),
                        "{\"carrier\":\"carrier-a\",\"quote_cents\":1500}".getBytes(StandardCharsets.UTF_8));
                }
            }).subscribe("shipping.quote");
            // NATS-DOC-END

            Thread.sleep(200);
            Message reply = nc.request("shipping.quote", null, Duration.ofSeconds(2));
            if (reply != null) {
                System.out.println("quote: " + new String(reply.getData(), StandardCharsets.UTF_8));
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (IOException e) {
            // can be thrown by connect
        }
    }
}
