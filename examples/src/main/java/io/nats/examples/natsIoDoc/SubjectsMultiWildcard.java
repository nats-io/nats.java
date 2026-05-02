package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;

import java.io.IOException;

public class SubjectsMultiWildcard {
    // NATS-DOC-START
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("demo.nats.io:4222")) {

            // NATS-DOC-START
            // Subscribe to the shipped orders
            Dispatcher dShipped = nc.createDispatcher(msg -> {
                System.out.printf("[sensor.alarm.*]        %-15s (%s)\n", new String(msg.getData()),  msg.getSubject());
            });
            dShipped.subscribe("sensor.alarm.*");

            Dispatcher dPlaced = nc.createDispatcher(msg -> {
                System.out.printf("[sensor.*.*.critical]   %-15s (%s)\n", new String(msg.getData()),  msg.getSubject());
            });
            dPlaced.subscribe("sensor.*.*.critical");

            // Subscribe to the retail orders
            Dispatcher dRetail = nc.createDispatcher(msg -> {
                System.out.printf("[sensor.>]              %-15s (%s)\n", new String(msg.getData()),  msg.getSubject());
            });
            dRetail.subscribe("sensor.>");

            // Publish messages to the various subjects
            nc.publish("sensor.alarm.smoke", "kitchen,14:22".getBytes());
            nc.publish("sensor.alarm.smoke.critical", "kitchen,14:23".getBytes());
            nc.publish("sensor.alarm.water", "basement,16:42".getBytes());
            nc.publish("sensor.alarm.water.critical", "basement,16:43".getBytes());
            // NATS-DOC-END
            Thread.sleep(500);
        }
        catch (InterruptedException e) {
            // can be thrown by connect
            Thread.currentThread().interrupt();
        }
        catch (IOException e) {
            // can be thrown by connect
        }
    }
    // NATS-DOC-END
}