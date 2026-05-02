package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Nats;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class RequestReplyBasic {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("demo.nats.io:4222")) {


            // Set up a service
            Dispatcher dTime = nc.createDispatcher(msg -> {
                nc.publish(msg.getReplyTo(), ZonedDateTime.now()
                    .withZoneSameInstant(ZoneId.of("GMT"))
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.nnnnnnnnn'Z'"))
                    .getBytes(StandardCharsets.ISO_8859_1));
            });
            dTime.subscribe("time");

            // Make a request with a timeout and direct response
            Message m = nc.request("time", null, Duration.ofSeconds(1));
            if (m == null) {
                System.out.println("Request failed, no response.");
            }
            else {
                System.out.println("Response: " + new String(m.getData()));
            }
            // NATS-DOC-END
        }
        catch (InterruptedException e) {
            // can be thrown by connect
            Thread.currentThread().interrupt();
        }
        catch (IOException e) {
            // can be thrown by connect
        }
    }
}