package io.nats.client.proto;

import io.nats.client.*;
import io.nats.client.impl.VertxDataPort;
import io.nats.client.support.Status;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class Proto {

    public static void main(String [] args) {
        try {

            final Options.Builder builder = new Options.Builder();
            builder.dataPortType(VertxDataPort.class.getCanonicalName());
            builder.errorListener(new ErrorListener() {
                @Override
                public void errorOccurred(Connection conn, String error) {
                    System.out.println("ERROR OCCURRED: " + error);
                }

                @Override
                public void exceptionOccurred(Connection conn, Exception exp) {
                    exp.printStackTrace();
                }

                @Override
                public void slowConsumerDetected(Connection conn, Consumer consumer) {
                    System.out.println("Slow consumer");
                }

                @Override
                public void messageDiscarded(Connection conn, Message msg) {
                    System.out.println("messageDiscarded " + msg);
                }

                @Override
                public void heartbeatAlarm(Connection conn, JetStreamSubscription sub, long lastStreamSequence, long lastConsumerSequence) {
                    ErrorListener.super.heartbeatAlarm(conn, sub, lastStreamSequence, lastConsumerSequence);
                }

                @Override
                public void unhandledStatus(Connection conn, JetStreamSubscription sub, Status status) {
                    ErrorListener.super.unhandledStatus(conn, sub, status);
                }

                @Override
                public void flowControlProcessed(Connection conn, JetStreamSubscription sub, String subject, FlowControlSource source) {
                    ErrorListener.super.flowControlProcessed(conn, sub, subject, source);
                }
            });

            builder.connectionTimeout(Duration.ofSeconds(30));
            final Connection connect1 = Nats.connect(builder.build());

            final Subscription subscription = connect1.subscribe("foo");

            final Connection connect2 = Nats.connect(builder.build());

            connect2.publish("foo", "bar".getBytes(StandardCharsets.UTF_8));

            Message message = subscription.nextMessage(Duration.ofSeconds(30));

            System.out.println("MESSAGE from " + message.getSubject());

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
