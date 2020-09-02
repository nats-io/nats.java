package io.nats.client.impl;

import io.nats.client.*;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;


public class SendHPubTest {

    @Test
    public void testNoHeader() throws Exception {

        final NatsTestServer natsTestServer = new NatsTestServer(true);
        final Connection connect1 = Nats.connect(natsTestServer.getURI());
        final Connection connect2 = Nats.connect(natsTestServer.getURI());

        final String subject = "foo";

        try {

            Thread.sleep(1000);
            System.out.println(connect1.getConnectedUrl());

            final Subscription subscribe = connect2.subscribe(subject);

            connect1.publish(subject, "foo".getBytes(StandardCharsets.UTF_8));


            final Message message = subscribe.nextMessage(Duration.ofSeconds(10));

            assertNotNull(message);

            assertEquals("foo", new String(message.getData(), StandardCharsets.UTF_8));

        } finally {
            connect1.close();
            connect2.close();
            natsTestServer.close();
        }




    }


    @Test
    public void testWithMessageBuilderNoHeader() throws Exception {

        final NatsTestServer natsTestServer = new NatsTestServer(true);
        final Connection connect1 = Nats.connect(natsTestServer.getURI());
        final Connection connect2 = Nats.connect(natsTestServer.getURI());

        final String subject = "foo";

        try {

            Thread.sleep(1000);
            System.out.println(connect1.getConnectedUrl());

            final Subscription subscribe = connect2.subscribe(subject);

            connect1.publish(Nats.messageBuilder().withData("foo".getBytes(StandardCharsets.UTF_8)).withSubject(subject).build());

            final Message message = subscribe.nextMessage(Duration.ofSeconds(10));

            assertNotNull(message);

            assertEquals("foo", new String(message.getData(), StandardCharsets.UTF_8));

        } finally {
            connect1.close();
            connect2.close();
            natsTestServer.close();
        }
    }

    //@Test
    public void testWithMessageBuilderWithHeader() throws Exception {

        final NatsTestServer natsTestServer = new NatsTestServer(true);
        final Connection connect1 = Nats.connect(natsTestServer.getURI());
        final Connection connect2 = Nats.connect(natsTestServer.getURI());

        final String subject = "foo";

        try {

            Thread.sleep(1000);
            System.out.println(connect1.getConnectedUrl());

            final Subscription subscribe = connect2.subscribe(subject);

            connect1.publish(Nats.messageBuilder()
                    .withData("foo".getBytes(StandardCharsets.UTF_8))
                    .withSubject(subject).addHeader("foo", "bar").build());

            final Message message = subscribe.nextMessage(Duration.ofSeconds(10));

            assertNotNull(message);

            assertEquals("foo", new String(message.getData(), StandardCharsets.UTF_8));

        } finally {
            connect1.close();
            connect2.close();
            natsTestServer.close();
        }
    }
}
