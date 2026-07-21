package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Nats;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class LearnCoreNatsRequestReplyRespond {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {
            // NATS-DOC-START
            // The inventory service: subscribe to orders.inventory.check and answer
            // every request by publishing back to the reply subject it carries.
            nc.createDispatcher(msg -> {
                if (msg.getReplyTo() != null) {
                    nc.publish(msg.getReplyTo(),
                        "{\"in_stock\":true,\"warehouse\":\"us-east\"}".getBytes(StandardCharsets.UTF_8));
                }
            }).subscribe("orders.inventory.check");
            // NATS-DOC-END

            Thread.sleep(200);
            Message reply = nc.request("orders.inventory.check", null, Duration.ofSeconds(2));
            if (reply != null) {
                System.out.println("inventory replied: " + new String(reply.getData(), StandardCharsets.UTF_8));
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
